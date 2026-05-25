//! In-memory `ObjectStore` for tests and the integration suite (design §11).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::{HeadToken, ObjectStore};
use crate::{Error, Hash, Result};

#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<HashMap<Hash, Bytes>>,
    head: Mutex<Option<Hash>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.objects
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                bucket: "memory".into(),
                key: hash.as_hex().to_string(),
            })
    }

    async fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        self.objects.lock().unwrap().insert(hash.clone(), bytes);
        Ok(())
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(hash))
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.objects.lock().unwrap().remove(hash);
        Ok(())
    }

    async fn get_head(&self) -> Result<Option<Hash>> {
        Ok(self.head.lock().unwrap().clone())
    }

    async fn put_head(&self, head: Option<&Hash>) -> Result<()> {
        *self.head.lock().unwrap() = head.cloned();
        Ok(())
    }

    async fn get_head_with_token(&self) -> Result<(Option<Hash>, HeadToken)> {
        let head = self.head.lock().unwrap().clone();
        Ok((head.clone(), token_for(head.as_ref())))
    }

    async fn compare_and_set_head(
        &self,
        expected: &HeadToken,
        new: Option<&Hash>,
    ) -> Result<HeadToken> {
        let mut guard = self.head.lock().unwrap();
        let current_token = token_for(guard.as_ref());
        if &current_token != expected {
            return Err(Error::PreconditionFailed {
                bucket: "memory".into(),
                key: "HEAD".into(),
            });
        }
        *guard = new.cloned();
        Ok(token_for(new))
    }
}

fn token_for(head: Option<&Hash>) -> HeadToken {
    match head {
        Some(h) => HeadToken::new(h.as_hex().to_string()),
        None => HeadToken::absent(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conforms() {
        let store = MemoryStore::new();
        crate::store::conformance::round_trip(&store).await;
        let store = MemoryStore::new();
        crate::store::conformance::missing_returns_not_found(&store).await;
        let store = MemoryStore::new();
        crate::store::conformance::list_returns_all_keys(&store).await;
        let store = MemoryStore::new();
        crate::store::conformance::head_round_trip(&store).await;
        let store = MemoryStore::new();
        crate::store::conformance::cas_head_serializes_writers(&store).await;
    }
}
