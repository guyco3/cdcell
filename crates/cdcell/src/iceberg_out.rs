//! Iceberg output through a REST catalog (design §8, §11.2).
//!
//! One table per (script, class, SQLite table), append-only, format v2,
//! partitioned by `day(ce_time)`, `bucket(16, _cell_id)`. Rows arrive as
//! [`OutRow`] (a name → value tree) and are converted to Arrow by field name
//! against the schema the catalog returns, so catalog-assigned field ids are
//! honoured.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow_array::builder::*;
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use iceberg::spec::{
    Datum, Literal, MapType, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, StructType, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use cdcell_core::schema::{Affinity, TableInfo};

/// A value in an output row (a JSON-like tree typed for Iceberg).
#[derive(Clone, Debug, PartialEq)]
pub enum OutValue {
    Null,
    Long(i64),
    Double(f64),
    Str(String),
    Bin(Vec<u8>),
    Bool(bool),
    /// microseconds since the epoch, UTC
    Ts(i64),
    Struct(BTreeMap<String, OutValue>),
    Map(Vec<(String, String)>),
}

pub type OutRow = BTreeMap<String, OutValue>;

/// Iceberg type for a SQLite column (design §8.3).
pub fn column_type(affinity: Affinity) -> PrimitiveType {
    match affinity {
        Affinity::Integer => PrimitiveType::Long,
        Affinity::Real | Affinity::Numeric => PrimitiveType::Double,
        Affinity::Blob => PrimitiveType::Binary,
        Affinity::Text | Affinity::Untyped => PrimitiveType::String,
    }
}

struct Ids(i32);
impl Ids {
    fn next(&mut self) -> i32 {
        self.0 += 1;
        self.0
    }
}

fn opt(ids: &mut Ids, name: &str, t: Type) -> Arc<NestedField> {
    Arc::new(NestedField::optional(ids.next(), name, t))
}
fn req(ids: &mut Ids, name: &str, t: Type) -> Arc<NestedField> {
    Arc::new(NestedField::required(ids.next(), name, t))
}
fn prim(p: PrimitiveType) -> Type {
    Type::Primitive(p)
}

fn row_struct(ids: &mut Ids, info: &TableInfo) -> Type {
    Type::Struct(StructType::new(info.columns.iter().map(|c| opt(ids, &c.name, prim(column_type(c.affinity)))).collect()))
}

fn key_struct(ids: &mut Ids, info: &TableInfo) -> Type {
    if info.pk.is_empty() {
        Type::Struct(StructType::new(vec![opt(ids, "rowid", prim(PrimitiveType::Long))]))
    } else {
        Type::Struct(StructType::new(
            info.pk.iter().map(|&c| opt(ids, &info.columns[c].name, prim(column_type(info.columns[c].affinity)))).collect(),
        ))
    }
}

/// The frozen v1 change-table schema (design §8.2) for one SQLite table.
pub fn change_schema(info: &TableInfo) -> Result<Schema> {
    use PrimitiveType::*;
    let mut ids = Ids(0);
    let i = &mut ids;
    let mut fields = vec![
        req(i, "ce_id", prim(String)),
        req(i, "ce_source", prim(String)),
        req(i, "ce_specversion", prim(String)),
        req(i, "ce_type", prim(String)),
        req(i, "ce_subject", prim(String)),
        req(i, "ce_time", prim(Timestamptz)),
        req(i, "ce_datacontenttype", prim(String)),
        req(i, "ce_dataschema", prim(String)),
        req(i, "ce_sequence", prim(String)),
        req(i, "ce_partitionkey", prim(String)),
        req(i, "ce_recordedtime", prim(Timestamptz)),
        req(i, "_cell_id", prim(String)),
        req(i, "_epoch", prim(Long)),
        req(i, "_txid", prim(Long)),
        req(i, "_key_json", prim(String)),
    ];
    let k = key_struct(i, info);
    fields.push(opt(i, "_key", k));
    let k = key_struct(i, info);
    fields.push(opt(i, "_old_key", k));
    let k = key_struct(i, info);
    fields.push(opt(i, "_new_key", k));
    fields.push(req(i, "op", prim(String)));
    let b = row_struct(i, info);
    fields.push(opt(i, "before", b));
    let a = row_struct(i, info);
    fields.push(opt(i, "after", a));
    let (kid, vid) = (i.next(), i.next());
    fields.push(opt(
        i,
        "_raw",
        Type::Map(MapType::new(
            Arc::new(NestedField::map_key_element(kid, prim(String))),
            Arc::new(NestedField::map_value_element(vid, prim(String), false)),
        )),
    ));
    let source = StructType::new(vec![
        opt(i, "version", prim(String)),
        opt(i, "connector", prim(String)),
        opt(i, "name", prim(String)),
        opt(i, "ts_ms", prim(Long)),
        opt(i, "ts_us", prim(Long)),
        opt(i, "snapshot", prim(String)),
        opt(i, "db", prim(String)),
        opt(i, "sequence", prim(String)),
        opt(i, "table", prim(String)),
        opt(i, "change_id", prim(Long)),
        opt(i, "script", prim(String)),
        opt(i, "cell_class", prim(String)),
        opt(i, "cell_id", prim(String)),
        opt(i, "epoch", prim(Long)),
        opt(i, "txid", prim(Long)),
        opt(i, "synthetic", prim(Boolean)),
    ]);
    fields.push(opt(i, "source", Type::Struct(source)));
    let tx = StructType::new(vec![opt(i, "id", prim(String)), opt(i, "total_order", prim(Long)), opt(i, "data_collection_order", prim(Long))]);
    fields.push(opt(i, "transaction", Type::Struct(tx)));
    fields.push(req(i, "ts_ms", prim(Long)));
    fields.push(req(i, "ts_us", prim(Long)));
    Schema::builder().with_fields(fields).build().context("change schema")
}

/// `_cdcell_kv` (design §8.7): the change-table envelope with
/// `_key struct<k>` and value images `struct<value_type, value_json, value_raw>`.
pub fn kv_schema() -> Result<Schema> {
    let base = change_schema(&TableInfo {
        name: "_cdcell_kv".into(),
        root: 0,
        sql: String::new(),
        without_rowid: true,
        columns: vec![cdcell_core::schema::Column {
            name: "k".into(),
            decl_type: "TEXT".into(),
            affinity: Affinity::Text,
            not_null: true,
            default: cdcell_core::Value::Null,
            generated: None,
        }],
        record_order: vec![0],
        rowid_alias: None,
        pk: vec![0],
    })?;
    // Replace before/after with the value image struct.
    let mut ids = Ids(base.highest_field_id());
    let mut fields: Vec<Arc<NestedField>> = Vec::new();
    for f in base.as_struct().fields() {
        if f.name == "before" || f.name == "after" {
            let image = Type::Struct(StructType::new(vec![
                opt(&mut ids, "value_type", prim(PrimitiveType::String)),
                opt(&mut ids, "value_json", prim(PrimitiveType::String)),
                opt(&mut ids, "value_raw", prim(PrimitiveType::Binary)),
            ]));
            fields.push(Arc::new(NestedField::optional(f.id, &f.name, image)));
        } else {
            fields.push(f.clone());
        }
    }
    Schema::builder().with_fields(fields).build().context("kv schema")
}

/// `_cdcell_progress` (design §8.8).
pub fn progress_schema() -> Result<Schema> {
    use PrimitiveType::*;
    let mut i = Ids(0);
    Schema::builder()
        .with_fields(vec![
            req(&mut i, "_cell_id", prim(String)),
            req(&mut i, "_epoch", prim(Long)),
            req(&mut i, "_txid", prim(Long)),
            req(&mut i, "ce_time", prim(Timestamptz)),
            req(&mut i, "committed_at", prim(Timestamptz)),
            req(&mut i, "batch", prim(Long)),
        ])
        .build()
        .context("progress schema")
}

/// Identifier rule (design §8.1): lowercase, non-[a-z0-9_] → `_`.
pub fn ident(name: &str) -> String {
    let s: String = name.to_ascii_lowercase().chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if s.starts_with("_cdcell_") {
        format!("u_{s}")
    } else {
        s
    }
}

pub struct Sink {
    catalog: Arc<dyn Catalog>,
}

impl Sink {
    pub async fn connect(uri: &str, warehouse: &str) -> Result<Sink> {
        // S3 via OpenDAL; table credentials are vended by the catalog (STS).
        let storage: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(iceberg_storage_opendal::OpenDalStorageFactory::S3 { customized_credential_load: None });
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage)
            .load("lakekeeper", HashMap::from([("uri".to_string(), uri.to_string()), ("warehouse".to_string(), warehouse.to_string())]))
            .await
            .context("connect to the REST catalog")?;
        Ok(Sink { catalog: Arc::new(catalog) })
    }

    pub async fn ensure_namespace(&self, levels: &[String]) -> Result<()> {
        for n in 1..=levels.len() {
            let ns = NamespaceIdent::from_vec(levels[..n].to_vec())?;
            if !self.catalog.namespace_exists(&ns).await? {
                match self.catalog.create_namespace(&ns, HashMap::new()).await {
                    Ok(_) => {}
                    Err(e) if self.catalog.namespace_exists(&ns).await.unwrap_or(false) => drop(e),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }

    /// Creates the table if missing (partitioned per design §8.1 when it has
    /// `ce_time` and `_cell_id`).
    pub async fn ensure_table(&self, ns: &[String], name: &str, schema: Schema, props: HashMap<String, String>) -> Result<TableIdent> {
        self.ensure_namespace(ns).await?;
        let ident = TableIdent::new(NamespaceIdent::from_vec(ns.to_vec())?, name.to_string());
        if self.catalog.table_exists(&ident).await? {
            return Ok(ident);
        }
        let mut builder = UnboundPartitionSpec::builder();
        if let (Some(t), Some(c)) = (schema.field_by_name("ce_time"), schema.field_by_name("_cell_id")) {
            builder = builder
                .add_partition_field(t.id, "ce_time_day", Transform::Day)?
                .add_partition_field(c.id, "cell_bucket", Transform::Bucket(16))?;
        }
        let mut properties = props;
        properties.insert("format-version".into(), "2".into());
        let creation = TableCreation::builder().name(name.to_string()).schema(schema).partition_spec(builder.build()).properties(properties).build();
        match self.catalog.create_table(&ident.namespace, creation).await {
            Ok(_) => Ok(ident),
            Err(e) if self.catalog.table_exists(&ident).await.unwrap_or(false) => {
                drop(e);
                Ok(ident)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Appends rows in one snapshot carrying `snapshot_props` (e.g.
    /// `cdcell.batch`). Rows are grouped by partition.
    pub async fn append(&self, ident: &TableIdent, rows: &[OutRow], snapshot_props: HashMap<String, String>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let table = self.catalog.load_table(ident).await?;
        let schema = table.metadata().current_schema().clone();
        let spec: Arc<PartitionSpec> = table.metadata().default_partition_spec().clone();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);

        // group by partition value
        let transforms: Vec<_> = spec.fields().iter().map(|f| iceberg::transform::create_transform_function(&f.transform)).collect::<iceberg::Result<_>>()?;
        let mut groups: BTreeMap<String, (Option<Struct>, Vec<&OutRow>)> = BTreeMap::new();
        for row in rows {
            let mut lits = Vec::new();
            let mut label = String::new();
            for (f, t) in spec.fields().iter().zip(&transforms) {
                let src = schema.field_by_id(f.source_id).ok_or_else(|| anyhow!("partition source field"))?;
                let datum = match row.get(&src.name) {
                    Some(OutValue::Ts(us)) => Datum::timestamptz_micros(*us),
                    Some(OutValue::Str(s)) => Datum::string(s),
                    Some(OutValue::Long(v)) => Datum::long(*v),
                    other => return Err(anyhow!("unsupported partition source value {other:?}")),
                };
                let out = t.transform_literal(&datum)?;
                label.push_str(&format!("{out:?}/"));
                lits.push(out.map(|d| Literal::Primitive(d.literal().clone())));
            }
            let key = (!spec.fields().is_empty()).then(|| Struct::from_iter(lits));
            groups.entry(label).or_insert_with(|| (key, Vec::new())).1.push(row);
        }

        let mut data_files = Vec::new();
        for (_, (key, group)) in groups {
            let batch = to_batch(&arrow_schema, &group)?;
            let pkey = key.map(|k| iceberg::spec::PartitionKey::new((*spec).clone(), schema.clone(), k));
            let parquet = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
            let rolling = RollingFileWriterBuilder::new_with_default_file_size(
                parquet,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())?,
                // Unique per data file: a reused name would overwrite an
                // existing file (Iceberg rejects it only as a duplicate add).
                DefaultFileNameGenerator::new(unique_file_prefix(), None, iceberg::spec::DataFileFormat::Parquet),
            );
            let mut writer = DataFileWriterBuilder::new(rolling).build(pkey).await?;
            writer.write(batch).await?;
            data_files.extend(writer.close().await?);
        }
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files).set_snapshot_properties(snapshot_props);
        let tx = action.apply(tx)?;
        tx.commit(self.catalog.as_ref()).await?;
        Ok(())
    }
}

fn unique_file_prefix() -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros();
    format!("cdcell-{now}-{}-{}", std::process::id(), SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
}

fn to_batch(schema: &Arc<arrow_schema::Schema>, rows: &[&OutRow]) -> Result<RecordBatch> {
    let cols: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| build(f, &rows.iter().map(|r| r.get(f.name())).collect::<Vec<_>>()))
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

/// Builds one Arrow array for `field` from per-row values (by name).
fn build(field: &Field, vals: &[Option<&OutValue>]) -> Result<ArrayRef> {
    let null = |v: &Option<&OutValue>| matches!(v, None | Some(OutValue::Null));
    Ok(match field.data_type() {
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for v in vals {
                match v {
                    Some(OutValue::Long(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = Int32Builder::new();
            for v in vals {
                match v {
                    Some(OutValue::Long(x)) => b.append_value(*x as i32),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for v in vals {
                match v {
                    Some(OutValue::Double(x)) => b.append_value(*x),
                    Some(OutValue::Long(x)) => b.append_value(*x as f64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for v in vals {
                match v {
                    Some(OutValue::Bool(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for v in vals {
                match v {
                    Some(OutValue::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::LargeUtf8 => {
            let mut b = LargeStringBuilder::new();
            for v in vals {
                match v {
                    Some(OutValue::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for v in vals {
                match v {
                    Some(OutValue::Bin(x)) => b.append_value(x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::LargeBinary => {
            let mut b = LargeBinaryBuilder::new();
            for v in vals {
                match v {
                    Some(OutValue::Bin(x)) => b.append_value(x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in vals {
                match v {
                    Some(OutValue::Ts(x)) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish().with_timezone_opt(tz.clone()))
        }
        DataType::Struct(children) => {
            let child_arrays: Vec<ArrayRef> = children
                .iter()
                .map(|c| {
                    let sub: Vec<Option<&OutValue>> = vals
                        .iter()
                        .map(|v| match v {
                            Some(OutValue::Struct(m)) => m.get(c.name()),
                            _ => None,
                        })
                        .collect();
                    build(c, &sub)
                })
                .collect::<Result<_>>()?;
            let nulls = arrow_buffer_nulls(vals.iter().map(|v| !null(v)));
            Arc::new(StructArray::try_new(children.clone(), child_arrays, nulls)?)
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(kv) = entries.data_type() else { return Err(anyhow!("map entries")) };
            let mut offsets = vec![0i32];
            let (mut keys, mut values) = (Vec::new(), Vec::new());
            for v in vals {
                if let Some(OutValue::Map(m)) = v {
                    for (k, val) in m {
                        keys.push(OutValue::Str(k.clone()));
                        values.push(OutValue::Str(val.clone()));
                    }
                }
                offsets.push(keys.len() as i32);
            }
            let ka = build(&kv[0], &keys.iter().map(Some).collect::<Vec<_>>())?;
            let va = build(&kv[1], &values.iter().map(Some).collect::<Vec<_>>())?;
            let entry = StructArray::try_new(Fields::from(kv.to_vec()), vec![ka, va], None)?;
            let nulls = arrow_buffer_nulls(vals.iter().map(|v| !null(v)));
            Arc::new(arrow_array::MapArray::try_new(entries.clone(), arrow_buffer_offsets(offsets), entry, nulls, *sorted)?)
        }
        other => return Err(anyhow!("unsupported arrow type {other:?} for {}", field.name())),
    })
}

fn arrow_buffer_nulls(valid: impl Iterator<Item = bool>) -> Option<arrow_buffer::NullBuffer> {
    let v: Vec<bool> = valid.collect();
    if v.iter().all(|b| *b) {
        None
    } else {
        Some(arrow_buffer::NullBuffer::from(v))
    }
}

fn arrow_buffer_offsets(offsets: Vec<i32>) -> arrow_buffer::OffsetBuffer<i32> {
    arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(offsets))
}
