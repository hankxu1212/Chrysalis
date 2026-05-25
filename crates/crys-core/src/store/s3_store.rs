//! S3-backed `ObjectStore`.
//!
//! Owns one Chrysalis repo's prefix on S3 (design §5):
//!
//! ```text
//! s3://<bucket>/<prefix>/
//! ├── HEAD
//! ├── config.json
//! └── objects/<ab>/<cdef…>
//! ```
//!
//! Picks multipart upload for chunk-sized payloads automatically via
//! [`S3Client::put_multipart`] (design §8 step 4).

use async_trait::async_trait;
use bytes::Bytes;

use super::ObjectStore;
use crate::s3::S3Client;
use crate::{Error, Hash, Result, S3Uri};

const HEAD_KEY: &str = "HEAD";
const OBJECTS_PREFIX: &str = "objects";

#[derive(Debug, Clone)]
pub struct S3Store {
    client: S3Client,
    bucket: String,
    /// Prefix without trailing slash. Empty string = bucket root.
    prefix: String,
}

impl S3Store {
    /// Build a store rooted at `s3://<uri.bucket>/<uri.key>/`.
    pub fn new(client: S3Client, uri: S3Uri) -> Self {
        let prefix = uri.key.trim_end_matches('/').to_string();
        Self {
            client,
            bucket: uri.bucket,
            prefix,
        }
    }

    /// `S3Uri` form of this store's root, useful for logging.
    pub fn root(&self) -> S3Uri {
        S3Uri {
            bucket: self.bucket.clone(),
            key: self.prefix.clone(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn client(&self) -> &S3Client {
        &self.client
    }

    fn join(&self, segment: &str) -> String {
        if self.prefix.is_empty() {
            segment.to_string()
        } else {
            format!("{}/{}", self.prefix, segment)
        }
    }

    fn object_key(&self, hash: &Hash) -> String {
        self.join(&format!("{OBJECTS_PREFIX}/{}", hash.storage_path()))
    }

    fn head_key(&self) -> String {
        self.join(HEAD_KEY)
    }

    fn objects_prefix(&self) -> String {
        self.join(OBJECTS_PREFIX)
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.client.get(&self.bucket, &self.object_key(hash)).await
    }

    async fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        self.client
            .put_multipart(&self.bucket, &self.object_key(hash), bytes)
            .await
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        self.client.head(&self.bucket, &self.object_key(hash)).await
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        let prefix = format!("{}/", self.objects_prefix());
        let keys = self.client.list_prefix(&self.bucket, &prefix).await?;
        let mut hashes = Vec::with_capacity(keys.len());
        for key in keys {
            // Expect `<prefix>/objects/<ab>/<cdef…>` — pull off the first two
            // components past the objects prefix and reassemble the hex.
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            let mut parts = rest.split('/');
            match (parts.next(), parts.next(), parts.next()) {
                (Some(ab), Some(cdef), None) if ab.len() == 2 && cdef.len() == 62 => {
                    if let Ok(hash) = Hash::from_hex(format!("{ab}{cdef}")) {
                        hashes.push(hash);
                    }
                }
                _ => continue,
            }
        }
        Ok(hashes)
    }

    async fn delete(&self, _hash: &Hash) -> Result<()> {
        Err(Error::Io(std::io::Error::other(
            "deleting objects from the S3 store is not supported via this API",
        )))
    }

    async fn get_head(&self) -> Result<Option<Hash>> {
        let bytes = match self.client.get(&self.bucket, &self.head_key()).await {
            Ok(b) => b,
            Err(Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e),
        };
        let trimmed = std::str::from_utf8(&bytes)
            .map_err(|_| Error::InvalidHash("HEAD is not utf-8".into()))?
            .trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        Ok(Some(Hash::from_hex(trimmed)?))
    }

    async fn put_head(&self, head: Option<&Hash>) -> Result<()> {
        let bytes = head
            .map(|h| Bytes::copy_from_slice(h.as_hex().as_bytes()))
            .unwrap_or_default();
        self.client.put(&self.bucket, &self.head_key(), bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(prefix: &str) -> S3Store {
        let uri = S3Uri::parse(&format!("s3://bucket/{prefix}")).unwrap();
        // Construct without ever calling AWS; only `key()` helpers are exercised.
        S3Store {
            // Dummy client; will not be used.
            client: dummy_client(),
            bucket: uri.bucket,
            prefix: uri.key.trim_end_matches('/').to_string(),
        }
    }

    fn dummy_client() -> S3Client {
        // Build an SDK client with no credentials; key-routing tests never
        // hit the network.
        use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
        let creds = Credentials::new("AKIA-test", "secret", None, None, "test");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(creds)
            .region(Region::new("us-east-1"))
            .build();
        S3Client::from_sdk(aws_sdk_s3::Client::from_conf(config))
    }

    #[test]
    fn key_layout_with_prefix() {
        let s = store("repo");
        let hash = Hash::of(b"hello");
        assert_eq!(s.head_key(), "repo/HEAD");
        assert_eq!(s.objects_prefix(), "repo/objects");
        assert_eq!(
            s.object_key(&hash),
            format!("repo/objects/{}", hash.storage_path())
        );
    }

    #[test]
    fn key_layout_at_bucket_root() {
        let s = store("");
        let hash = Hash::of(b"hello");
        assert_eq!(s.head_key(), "HEAD");
        assert_eq!(s.objects_prefix(), "objects");
        assert_eq!(
            s.object_key(&hash),
            format!("objects/{}", hash.storage_path())
        );
    }

    #[test]
    fn trailing_slash_in_uri_is_ignored() {
        let s = store("repo/");
        assert_eq!(s.head_key(), "repo/HEAD");
    }
}
