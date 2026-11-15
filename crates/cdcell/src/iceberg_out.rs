//! Iceberg output through a REST catalog (design §2, §6).
//!
//! One table per (script, class, SQLite table), append-only, format v2,
//! partitioned by `day(__ts)`, `bucket(16, __cell_id)`. Rows arrive as
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
    Datum, Literal, MapType, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Transform,
    Type, UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
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
    /// microseconds since the epoch, UTC
    Ts(i64),
    Map(Vec<(String, String)>),
}

pub type OutRow = BTreeMap<String, OutValue>;

/// Iceberg type for a SQLite column (design §2).
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

/// Meta columns of every change-log table (design rev 5).
fn meta_fields(i: &mut Ids) -> Vec<Arc<NestedField>> {
    use PrimitiveType::*;
    vec![
        req(i, "__cell_id", prim(String)),
        req(i, "__epoch", prim(Long)),
        req(i, "__txid", prim(Long)),
        req(i, "__op", prim(String)),
        req(i, "__key", prim(String)),
        req(i, "__ts", prim(Timestamptz)),
        req(i, "__schema_version", prim(Long)),
        req(i, "__ingested_at", prim(Timestamptz)),
    ]
}

fn raw_field(i: &mut Ids) -> Arc<NestedField> {
    let (kid, vid) = (i.next(), i.next());
    opt(
        i,
        "__raw",
        Type::Map(MapType::new(
            Arc::new(NestedField::map_key_element(
                kid,
                prim(PrimitiveType::String),
            )),
            Arc::new(NestedField::map_value_element(
                vid,
                prim(PrimitiveType::String),
                false,
            )),
        )),
    )
}

/// The change-log schema for one SQLite table: meta columns, the table's
/// columns (typed by affinity, all nullable), `__raw`.
pub fn change_schema(info: &TableInfo) -> Result<Schema> {
    let mut ids = Ids(0);
    let i = &mut ids;
    let mut fields = meta_fields(i);
    for c in &info.columns {
        fields.push(opt(
            i,
            &crate::emit::out_col(&c.name),
            prim(column_type(c.affinity)),
        ));
    }
    fields.push(raw_field(i));
    Schema::builder()
        .with_fields(fields)
        .build()
        .context("change schema")
}

/// `_cdcell_kv` (design §2): meta columns, `k`, `value_type`,
/// `value_json`, `value_raw`.
pub fn kv_schema() -> Result<Schema> {
    use PrimitiveType::*;
    let mut ids = Ids(0);
    let i = &mut ids;
    let mut fields = meta_fields(i);
    fields.push(opt(i, "k", prim(String)));
    fields.push(opt(i, "value_type", prim(String)));
    fields.push(opt(i, "value_json", prim(String)));
    fields.push(opt(i, "value_raw", prim(Binary)));
    Schema::builder()
        .with_fields(fields)
        .build()
        .context("kv schema")
}

/// `_cdcell_progress` (design §2).
pub fn progress_schema() -> Result<Schema> {
    use PrimitiveType::*;
    let mut i = Ids(0);
    Schema::builder()
        .with_fields(vec![
            req(&mut i, "_cell_id", prim(String)),
            req(&mut i, "_epoch", prim(Long)),
            req(&mut i, "_txid", prim(Long)),
            req(&mut i, "captured_at", prim(Timestamptz)),
            req(&mut i, "committed_at", prim(Timestamptz)),
            req(&mut i, "batch", prim(Long)),
        ])
        .build()
        .context("progress schema")
}

/// Identifier rule (design §2): lowercase, non-[a-z0-9_] → `_`.
pub fn ident(name: &str) -> String {
    let s: String = name
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.starts_with("_cdcell_") {
        format!("u_{s}")
    } else {
        s
    }
}

/// Table properties cdcell maintains on its tables (design §8).
pub const TABLE_PROPERTIES: [(&str, &str); 2] = [
    ("write.metadata.delete-after-commit.enabled", "true"),
    ("write.metadata.previous-versions-max", "100"),
];

pub struct Sink {
    catalog: Arc<dyn Catalog>,
    uri: String,
    warehouse: String,
    http: reqwest::Client,
}

/// One table's staged change, ready for an atomic multi-table commit.
pub struct Staged {
    pub ident: TableIdent,
    pub requirements: Vec<iceberg::TableRequirement>,
    pub updates: Vec<iceberg::TableUpdate>,
}

impl Sink {
    pub async fn connect(uri: &str, warehouse: &str) -> Result<Sink> {
        // S3 via OpenDAL; table credentials are vended by the catalog (STS).
        let storage: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(iceberg_storage_opendal::OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            });
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage)
            .load(
                "lakekeeper",
                HashMap::from([
                    ("uri".to_string(), uri.to_string()),
                    ("warehouse".to_string(), warehouse.to_string()),
                ]),
            )
            .await
            .context("connect to the REST catalog")?;
        Ok(Sink {
            catalog: Arc::new(catalog),
            uri: uri.trim_end_matches('/').to_string(),
            warehouse: warehouse.to_string(),
            http: reqwest::Client::new(),
        })
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

    /// Creates the table if missing (partitioned by `day(__ts)`,
    /// `bucket(16, __cell_id)` when it has those columns, design §2).
    pub async fn ensure_table(
        &self,
        ns: &[String],
        name: &str,
        schema: Schema,
        props: HashMap<String, String>,
    ) -> Result<TableIdent> {
        self.ensure_namespace(ns).await?;
        let ident = TableIdent::new(NamespaceIdent::from_vec(ns.to_vec())?, name.to_string());
        if self.catalog.table_exists(&ident).await? {
            self.ensure_properties(&ident).await?;
            return Ok(ident);
        }
        let mut builder = UnboundPartitionSpec::builder();
        if let (Some(t), Some(c)) = (
            schema.field_by_name("__ts"),
            schema.field_by_name("__cell_id"),
        ) {
            builder = builder
                .add_partition_field(t.id, "ts_day", Transform::Day)?
                .add_partition_field(c.id, "cell_bucket", Transform::Bucket(16))?;
        }
        let mut properties = props;
        properties.insert("format-version".into(), "2".into());
        for (k, v) in TABLE_PROPERTIES {
            properties.insert(k.to_string(), v.to_string());
        }
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .partition_spec(builder.build())
            .properties(properties)
            .build();
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
    /// `cdcell.batch`), committed on its own.
    pub async fn append(
        &self,
        ident: &TableIdent,
        rows: &[OutRow],
        snapshot_props: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(staged) = self.stage(ident, rows, snapshot_props).await? {
            self.commit_atomic(vec![staged]).await?;
        }
        Ok(())
    }

    /// Writes the rows' data files (grouped by partition) and captures the
    /// table's append (requirements + updates) without committing it.
    pub async fn stage(
        &self,
        ident: &TableIdent,
        rows: &[OutRow],
        snapshot_props: HashMap<String, String>,
    ) -> Result<Option<Staged>> {
        if rows.is_empty() {
            return Ok(None);
        }
        let table = self.catalog.load_table(ident).await?;
        let schema = table.metadata().current_schema().clone();
        let spec: Arc<PartitionSpec> = table.metadata().default_partition_spec().clone();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);

        // group by partition value
        let transforms: Vec<_> = spec
            .fields()
            .iter()
            .map(|f| iceberg::transform::create_transform_function(&f.transform))
            .collect::<iceberg::Result<_>>()?;
        let mut groups: BTreeMap<String, (Option<Struct>, Vec<&OutRow>)> = BTreeMap::new();
        for row in rows {
            let mut lits = Vec::new();
            let mut label = String::new();
            for (f, t) in spec.fields().iter().zip(&transforms) {
                let src = schema
                    .field_by_id(f.source_id)
                    .ok_or_else(|| anyhow!("partition source field"))?;
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
            groups
                .entry(label)
                .or_insert_with(|| (key, Vec::new()))
                .1
                .push(row);
        }

        let mut data_files = Vec::new();
        for (_, (key, group)) in groups {
            let batch = to_batch(&arrow_schema, &group)?;
            let pkey =
                key.map(|k| iceberg::spec::PartitionKey::new((*spec).clone(), schema.clone(), k));
            let parquet =
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
            let rolling = RollingFileWriterBuilder::new_with_default_file_size(
                parquet,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())?,
                // Unique per data file: a reused name would overwrite an
                // existing file (Iceberg rejects it only as a duplicate add).
                DefaultFileNameGenerator::new(
                    unique_file_prefix(),
                    None,
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            let mut writer = DataFileWriterBuilder::new(rolling).build(pkey).await?;
            writer.write(batch).await?;
            data_files.extend(writer.close().await?);
        }
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(snapshot_props);
        let tx = action.apply(tx)?;
        let capture = crate::capture::Capturing::new(self.catalog.clone());
        match tx.commit(&capture).await {
            Err(e) if e.message() == crate::capture::CAPTURED => {}
            Err(e) => return Err(e.into()),
            Ok(_) => return Err(anyhow!("commit was not captured")),
        }
        let mut commit = capture
            .captured
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| anyhow!("no captured commit"))?;
        Ok(Some(Staged {
            ident: commit.identifier().clone(),
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        }))
    }

    /// One atomic multi-table commit (`POST /v1/{prefix}/transactions/commit`):
    /// all tables advance or none does.
    pub async fn commit_atomic(&self, staged: Vec<Staged>) -> Result<()> {
        if staged.is_empty() {
            return Ok(());
        }
        let cfg: serde_json::Value = self
            .http
            .get(format!("{}/v1/config", self.uri))
            .query(&[("warehouse", &self.warehouse)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let prefix = cfg["defaults"]["prefix"]
            .as_str()
            .or(cfg["overrides"]["prefix"].as_str())
            .ok_or_else(|| anyhow!("catalog prefix"))?
            .to_string();
        let changes: Vec<serde_json::Value> = staged
            .into_iter()
            .map(|s| {
                serde_json::json!({
                    "identifier": {"namespace": s.ident.namespace().clone().inner(), "name": s.ident.name()},
                    "requirements": s.requirements,
                    "updates": s.updates,
                })
            })
            .collect();
        let resp = self
            .http
            .post(format!("{}/v1/{prefix}/transactions/commit", self.uri))
            .json(&serde_json::json!({"table-changes": changes}))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(anyhow!(
                "multi-table commit failed: {status} {}",
                resp.text().await.unwrap_or_default()
            ));
        }
        Ok(())
    }

    /// Whether a table has a snapshot whose summary carries `cdcell.batch=seq`
    /// (restart roll-forward, design §6).
    pub async fn has_batch(&self, ident: &TableIdent, seq: u64) -> Result<bool> {
        if !self.catalog.table_exists(ident).await? {
            return Ok(false);
        }
        let table = self.catalog.load_table(ident).await?;
        let want = seq.to_string();
        let found = table
            .metadata()
            .snapshots()
            .any(|s| s.summary().additional_properties.get("cdcell.batch") == Some(&want));
        Ok(found)
    }

    /// Sets missing maintenance properties on an existing table.
    async fn ensure_properties(&self, ident: &TableIdent) -> Result<()> {
        let table = self.catalog.load_table(ident).await?;
        let props = table.metadata().properties();
        let missing: Vec<_> = TABLE_PROPERTIES
            .iter()
            .filter(|(k, v)| props.get(*k).map(String::as_str) != Some(*v))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let tx = Transaction::new(&table);
        let mut action = tx.update_table_properties();
        for (k, v) in missing {
            action = action.set(k.to_string(), v.to_string());
        }
        action.apply(tx)?.commit(self.catalog.as_ref()).await?;
        Ok(())
    }

    /// Schema evolution for one change-log table before a commit (design
    /// rev 5): maps each row's fingerprint to `__schema_version` through the
    /// registry (`cdcell.schemas` table property, fingerprint → version),
    /// adds missing columns, and moves values whose type conflicts with an
    /// existing column into `<col>__<type>`. Schema and registry are
    /// committed (additive, so readers never break) before the data.
    pub async fn evolve(&self, ident: &TableIdent, rows: &mut [OutRow]) -> Result<()> {
        let table = self.catalog.load_table(ident).await?;
        let schema = table.metadata().current_schema().clone();
        let mut registry: BTreeMap<String, i64> = table
            .metadata()
            .properties()
            .get("cdcell.schemas")
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let before = registry.clone();
        let mut add: BTreeMap<String, PrimitiveType> = BTreeMap::new();
        for row in rows.iter_mut() {
            if let Some(OutValue::Str(fp)) = row.remove(crate::emit::FINGERPRINT) {
                let next = registry.values().max().copied().unwrap_or(0) + 1;
                let v = *registry.entry(fp).or_insert(next);
                row.insert("__schema_version".into(), OutValue::Long(v));
            }
            let names: Vec<String> = row
                .keys()
                .filter(|k| !k.starts_with("__"))
                .cloned()
                .collect();
            for name in names {
                let Some(want) = value_type(&row[&name]) else {
                    continue;
                };
                let existing = schema.field_by_name(&name).map(|f| (*f.field_type).clone());
                match existing {
                    None => {
                        add.entry(name).or_insert(want);
                    }
                    Some(Type::Primitive(p)) if p == want => {}
                    Some(_) => {
                        let alt = format!("{name}__{}", type_suffix(&want));
                        let v = row.remove(&name).unwrap();
                        row.insert(alt.clone(), v);
                        if schema.field_by_name(&alt).is_none() {
                            add.entry(alt).or_insert(want);
                        }
                    }
                }
            }
        }
        if add.is_empty() && registry == before {
            return Ok(());
        }
        let tx = Transaction::new(&table);
        let mut tx = tx;
        if !add.is_empty() {
            let mut us = tx.update_schema();
            for (name, t) in &add {
                us = us.add_column(iceberg::transaction::AddColumn::optional(
                    name,
                    Type::Primitive(t.clone()),
                ));
            }
            tx = us.apply(tx)?;
        }
        if registry != before {
            tx = tx
                .update_table_properties()
                .set("cdcell.schemas".into(), serde_json::to_string(&registry)?)
                .apply(tx)?;
        }
        tx.commit(self.catalog.as_ref()).await?;
        Ok(())
    }

    /// Every table under a namespace prefix (recursive).
    /// Keys of `cell` whose latest change-log row is not a delete (resync,
    /// design §6). Empty when the table does not exist yet.
    pub async fn live_keys(
        &self,
        ns: &[String],
        name: &str,
        cell: &str,
    ) -> Result<std::collections::BTreeSet<String>> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        use futures::TryStreamExt;
        let ident = TableIdent::new(NamespaceIdent::from_vec(ns.to_vec())?, name.to_string());
        if !self.catalog.table_exists(&ident).await? {
            return Ok(Default::default());
        }
        let table = self.catalog.load_table(&ident).await?;
        if table.metadata().current_snapshot().is_none() {
            return Ok(Default::default());
        }
        let pred =
            iceberg::expr::Reference::new("__cell_id").equal_to(iceberg::spec::Datum::string(cell));
        let stream = table
            .scan()
            .select(["__cell_id", "__key", "__op", "__epoch", "__txid"])
            .with_filter(pred)
            .build()?
            .to_arrow()
            .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let mut rows = Vec::new();
        for b in &batches {
            let col = |n: &str| {
                b.column_by_name(n)
                    .ok_or_else(|| anyhow!("scan of {name}: no column {n}"))
            };
            let (c, k, o) = (
                col("__cell_id")?.as_string::<i32>(),
                col("__key")?.as_string::<i32>(),
                col("__op")?.as_string::<i32>(),
            );
            let (e, t) = (
                col("__epoch")?.as_primitive::<Int64Type>(),
                col("__txid")?.as_primitive::<Int64Type>(),
            );
            for i in 0..b.num_rows() {
                if c.value(i) == cell {
                    rows.push((
                        k.value(i).to_string(),
                        o.value(i).to_string(),
                        e.value(i),
                        t.value(i),
                    ));
                }
            }
        }
        Ok(crate::resync::live_keys(rows))
    }

    pub async fn tables_under(&self, root: &[String]) -> Result<Vec<TableIdent>> {
        let mut out = Vec::new();
        let mut stack = vec![NamespaceIdent::from_vec(root.to_vec())?];
        while let Some(ns) = stack.pop() {
            if !self.catalog.namespace_exists(&ns).await? {
                continue;
            }
            out.extend(self.catalog.list_tables(&ns).await?);
            stack.extend(self.catalog.list_namespaces(Some(&ns)).await?);
        }
        Ok(out)
    }

    /// Expires snapshots older than `older_than_ms` beyond the newest
    /// `retain_last`, and sets the maintenance properties (design §8).
    pub async fn maintain(
        &self,
        ident: &TableIdent,
        retain_last: usize,
        older_than_ms: i64,
    ) -> Result<(usize, usize)> {
        self.ensure_properties(ident).await?;
        let table = self.catalog.load_table(ident).await?;
        let before = table.metadata().snapshots().count();
        let tx = Transaction::new(&table);
        let action = tx
            .expire_snapshots()
            .retain_last(retain_last)
            .expire_older_than_ms(older_than_ms);
        let table = action.apply(tx)?.commit(self.catalog.as_ref()).await?;
        Ok((before, table.metadata().snapshots().count()))
    }

    pub fn table_ident(ns: &[String], name: &str) -> Result<TableIdent> {
        Ok(TableIdent::new(
            NamespaceIdent::from_vec(ns.to_vec())?,
            name.to_string(),
        ))
    }
}

fn value_type(v: &OutValue) -> Option<PrimitiveType> {
    Some(match v {
        OutValue::Long(_) => PrimitiveType::Long,
        OutValue::Double(_) => PrimitiveType::Double,
        OutValue::Str(_) => PrimitiveType::String,
        OutValue::Bin(_) => PrimitiveType::Binary,
        OutValue::Ts(_) => PrimitiveType::Timestamptz,
        OutValue::Null | OutValue::Map(_) => return None,
    })
}

fn type_suffix(t: &PrimitiveType) -> &'static str {
    match t {
        PrimitiveType::Long => "long",
        PrimitiveType::Double => "double",
        PrimitiveType::String => "string",
        PrimitiveType::Binary => "binary",
        _ => "value",
    }
}

fn unique_file_prefix() -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    format!(
        "cdcell-{now}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
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
        DataType::Map(entries, sorted) => {
            let DataType::Struct(kv) = entries.data_type() else {
                return Err(anyhow!("map entries"));
            };
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
            Arc::new(arrow_array::MapArray::try_new(
                entries.clone(),
                arrow_buffer_offsets(offsets),
                entry,
                nulls,
                *sorted,
            )?)
        }
        other => {
            return Err(anyhow!(
                "unsupported arrow type {other:?} for {}",
                field.name()
            ))
        }
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
