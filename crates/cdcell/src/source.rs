//! Object storage access: the celld bucket (read) and cdcell's state prefix.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl Store {
    pub fn s3(endpoint: &str, bucket: &str, prefix: &str, key: &str, secret: &str) -> Result<Store> {
        let s3 = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_allow_http(endpoint.starts_with("http://"))
            .with_virtual_hosted_style_request(false)
            .build()
            .context("S3 client")?;
        Ok(Store { inner: Arc::new(s3), prefix: prefix.to_string() })
    }

    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{}", self.prefix, key))
    }

    /// GET; `None` when the object no longer exists (GC'd, design §4.3).
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        match self.inner.get(&self.path(key)).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Keys under `prefix` (relative to the store prefix), sorted.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let p = self.path(prefix);
        let mut keys: Vec<String> = self
            .inner
            .list(Some(&p))
            .map_ok(|m| m.location.as_ref().strip_prefix(&self.prefix).unwrap_or(m.location.as_ref()).to_string())
            .try_collect()
            .await?;
        keys.sort();
        Ok(keys)
    }

    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<()> {
        self.inner.put(&self.path(key), PutPayload::from(body)).await?;
        Ok(())
    }
}
