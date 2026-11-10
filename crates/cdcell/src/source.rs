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
        crate::metrics::inc(&crate::metrics::M.s3_get);
        match self.inner.get(&self.path(key)).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => {
                crate::metrics::inc(&crate::metrics::M.not_found);
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Keys under `prefix` (relative to the store prefix), sorted.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        crate::metrics::inc(&crate::metrics::M.s3_list);
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

    /// Create-only PUT (If-None-Match: *). Ok(false) if the key exists.
    pub async fn put_new(&self, key: &str, body: Vec<u8>) -> Result<bool> {
        crate::metrics::inc(&crate::metrics::M.s3_put);
        let opts = object_store::PutOptions { mode: object_store::PutMode::Create, ..Default::default() };
        match self.inner.put_opts(&self.path(key), PutPayload::from(body), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// GET with the version needed for a conditional overwrite.
    pub async fn get_versioned(&self, key: &str) -> Result<Option<(Bytes, object_store::UpdateVersion)>> {
        crate::metrics::inc(&crate::metrics::M.s3_get);
        match self.inner.get(&self.path(key)).await {
            Ok(r) => {
                let v = object_store::UpdateVersion { e_tag: r.meta.e_tag.clone(), version: r.meta.version.clone() };
                Ok(Some((r.bytes().await?, v)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Conditional overwrite (If-Match). Ok(false) on a lost race.
    pub async fn put_if(&self, key: &str, body: Vec<u8>, version: object_store::UpdateVersion) -> Result<bool> {
        crate::metrics::inc(&crate::metrics::M.s3_put);
        let opts = object_store::PutOptions { mode: object_store::PutMode::Update(version), ..Default::default() };
        match self.inner.put_opts(&self.path(key), PutPayload::from(body), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<()> {
        crate::metrics::inc(&crate::metrics::M.s3_put);
        self.inner.put(&self.path(key), PutPayload::from(body)).await?;
        Ok(())
    }
}
