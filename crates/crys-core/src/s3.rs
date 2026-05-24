//! Core S3 client.
//!
//! Thin async wrapper around `aws-sdk-s3` exposing the operations Chrysalis
//! needs: `put`, `get`, `head`, `list`, `delete`, and a conditional-create
//! `put_if_absent` (used by `crys init` to refuse to clobber an existing
//! repo per design §8).
//!
//! Authentication uses the standard AWS credential chain via `aws-config`.
//! Bucket/region resolution is the caller's job — pass an [`S3Uri`] and a
//! configured [`Client`].

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as SdkClient;
use bytes::Bytes;

use crate::{Error, Result};

/// Default threshold above which `put_multipart_streaming` actually uses
/// multipart. Matches the design's default chunk size — there's no point in
/// multipart for sub-8 MB payloads.
pub const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;

/// Default part size for multipart uploads. AWS requires every part except
/// the last to be at least 5 MB; 8 MB matches the chunker's default chunk
/// size so each chunk maps cleanly to one part.
pub const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// Parsed `s3://bucket/key` URI.
///
/// Empty key (`s3://bucket` or `s3://bucket/`) is allowed and represents the
/// bucket root — useful for `list_prefix("")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Uri {
    pub bucket: String,
    pub key: String,
}

impl S3Uri {
    pub fn parse(uri: &str) -> Result<Self> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| Error::InvalidS3Uri(uri.into()))?;
        let (bucket, key) = match rest.split_once('/') {
            Some((b, k)) => (b, k),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            return Err(Error::InvalidS3Uri(uri.into()));
        }
        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// Append a path segment to this URI's key, joining with `/` as needed.
    pub fn join(&self, segment: &str) -> Self {
        let key = if self.key.is_empty() {
            segment.to_string()
        } else if self.key.ends_with('/') {
            format!("{}{}", self.key, segment)
        } else {
            format!("{}/{}", self.key, segment)
        };
        Self {
            bucket: self.bucket.clone(),
            key,
        }
    }
}

impl std::fmt::Display for S3Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.key.is_empty() {
            write!(f, "s3://{}", self.bucket)
        } else {
            write!(f, "s3://{}/{}", self.bucket, self.key)
        }
    }
}

/// Async S3 client wrapper.
#[derive(Debug, Clone)]
pub struct S3Client {
    inner: SdkClient,
}

impl S3Client {
    /// Build an `S3Client` from the default AWS credential chain.
    pub async fn from_env() -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self {
            inner: SdkClient::new(&config),
        }
    }

    /// Build an `S3Client` from an existing SDK client (e.g. for tests with a
    /// custom endpoint).
    pub fn from_sdk(inner: SdkClient) -> Self {
        Self { inner }
    }

    /// Upload `body` to `bucket/key`, overwriting any existing object.
    pub async fn put(&self, bucket: &str, key: &str, body: Bytes) -> Result<()> {
        self.inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| s3_err(e, bucket, key))?;
        Ok(())
    }

    /// Upload, automatically using multipart for large payloads.
    ///
    /// Switches to multipart when `body.len() > MULTIPART_THRESHOLD`. On
    /// failure mid-upload, the in-flight upload is best-effort aborted so we
    /// don't leak charges from orphaned parts (S3 retains parts until
    /// `AbortMultipartUpload` or the bucket lifecycle policy reaps them).
    pub async fn put_multipart(&self, bucket: &str, key: &str, body: Bytes) -> Result<()> {
        if body.len() <= MULTIPART_THRESHOLD {
            return self.put(bucket, key, body).await;
        }
        self.put_multipart_with_part_size(bucket, key, body, MULTIPART_PART_SIZE)
            .await
    }

    async fn put_multipart_with_part_size(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        part_size: usize,
    ) -> Result<()> {
        assert!(part_size > 0, "part_size must be > 0");

        let create = self
            .inner
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| s3_err(e, bucket, key))?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| Error::S3("create_multipart_upload returned no upload_id".into()))?
            .to_string();

        match self
            .upload_parts(bucket, key, &upload_id, body, part_size)
            .await
        {
            Ok(parts) => {
                let completed = CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();
                self.inner
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(completed)
                    .send()
                    .await
                    .map_err(|e| s3_err(e, bucket, key))?;
                Ok(())
            }
            Err(err) => {
                // Best-effort abort; surface the original error either way.
                let _ = self
                    .inner
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(err)
            }
        }
    }

    async fn upload_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        body: Bytes,
        part_size: usize,
    ) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        let total = body.len();
        let mut offset = 0usize;
        let mut part_number: i32 = 1;
        while offset < total {
            let end = (offset + part_size).min(total);
            let slice = body.slice(offset..end);
            let resp = self
                .inner
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(slice))
                .send()
                .await
                .map_err(|e| s3_err(e, bucket, key))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .set_e_tag(resp.e_tag().map(str::to_string))
                    .build(),
            );
            offset = end;
            part_number += 1;
        }
        Ok(parts)
    }

    /// Conditional create: succeeds only if the object does not already exist.
    ///
    /// Returns [`Error::PreconditionFailed`] if `bucket/key` is already
    /// present. Used by `crys init` to refuse to clobber an existing repo.
    pub async fn put_if_absent(&self, bucket: &str, key: &str, body: Bytes) -> Result<()> {
        let result = self
            .inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(body))
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if let SdkError::ServiceError(svc) = &e {
                    if svc.raw().status().as_u16() == 412 {
                        return Err(Error::PreconditionFailed {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        });
                    }
                }
                Err(s3_err(e, bucket, key))
            }
        }
    }

    /// Fetch `bucket/key` in full into memory.
    pub async fn get(&self, bucket: &str, key: &str) -> Result<Bytes> {
        let resp = self
            .inner
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| match &e {
                SdkError::ServiceError(svc)
                    if matches!(svc.err(), GetObjectError::NoSuchKey(_)) =>
                {
                    Error::NotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    }
                }
                _ => s3_err(e, bucket, key),
            })?;
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| Error::S3(format!("read body: {e}")))?
            .into_bytes();
        Ok(bytes)
    }

    /// Returns `true` iff `bucket/key` exists.
    pub async fn head(&self, bucket: &str, key: &str) -> Result<bool> {
        let result = self
            .inner
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(svc))
                if matches!(svc.err(), HeadObjectError::NotFound(_)) =>
            {
                Ok(false)
            }
            Err(e) => Err(s3_err(e, bucket, key)),
        }
    }

    /// List all keys under `prefix` (paginated internally; returns full list).
    pub async fn list_prefix(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self.inner.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(token) = continuation.as_deref() {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| s3_err(e, bucket, prefix))?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(str::to_string);
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(keys)
    }

    /// Delete `bucket/key`. Idempotent: no error if the object is already
    /// absent.
    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        self.inner
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| s3_err(e, bucket, key))?;
        Ok(())
    }
}

fn s3_err<E: std::fmt::Display, R>(err: SdkError<E, R>, bucket: &str, key: &str) -> Error {
    Error::S3(format!("{bucket}/{key}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_uris() {
        let u = S3Uri::parse("s3://my-bucket/path/to/key").unwrap();
        assert_eq!(u.bucket, "my-bucket");
        assert_eq!(u.key, "path/to/key");

        let bare = S3Uri::parse("s3://my-bucket").unwrap();
        assert_eq!(bare.bucket, "my-bucket");
        assert_eq!(bare.key, "");

        let trailing = S3Uri::parse("s3://my-bucket/").unwrap();
        assert_eq!(trailing.bucket, "my-bucket");
        assert_eq!(trailing.key, "");
    }

    #[test]
    fn rejects_invalid_uris() {
        assert!(S3Uri::parse("https://my-bucket/key").is_err());
        assert!(S3Uri::parse("s3:///key").is_err());
        assert!(S3Uri::parse("my-bucket/key").is_err());
    }

    #[test]
    fn join_appends_segments() {
        let base = S3Uri::parse("s3://b/prefix").unwrap();
        assert_eq!(base.join("HEAD").to_string(), "s3://b/prefix/HEAD");

        // Trailing slash is preserved in the key; join doesn't double up.
        let trailing = S3Uri::parse("s3://b/prefix/").unwrap();
        assert_eq!(trailing.join("HEAD").to_string(), "s3://b/prefix/HEAD");

        let bare = S3Uri::parse("s3://b").unwrap();
        assert_eq!(bare.join("HEAD").to_string(), "s3://b/HEAD");
    }

    #[test]
    fn display_round_trip() {
        for uri in ["s3://b/k", "s3://b/k/with/slashes", "s3://b"] {
            assert_eq!(S3Uri::parse(uri).unwrap().to_string(), uri);
        }
    }
}
