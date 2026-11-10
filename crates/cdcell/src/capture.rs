//! A catalog wrapper that captures table commits instead of sending them, so
//! several tables' changes can be committed in one atomic multi-table
//! transaction (design §6; Lakekeeper `POST /v1/{prefix}/transactions/commit`,
//! verified atomic in M0). iceberg-rust 0.10 only commits one table at a time.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

pub const CAPTURED: &str = "cdcell: commit captured for a multi-table transaction";

#[derive(Debug)]
pub struct Capturing {
    inner: Arc<dyn Catalog>,
    pub captured: Mutex<Vec<TableCommit>>,
}

impl Capturing {
    pub fn new(inner: Arc<dyn Catalog>) -> Capturing {
        Capturing {
            inner,
            captured: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Catalog for Capturing {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.inner.load_table(table).await
    }
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }
    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.captured.lock().unwrap().push(commit);
        Err(Error::new(ErrorKind::DataInvalid, CAPTURED))
    }
}
