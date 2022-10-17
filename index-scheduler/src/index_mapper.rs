use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::{fs, thread};

use log::error;
use meilisearch_types::heed::types::{SerdeBincode, Str};
use meilisearch_types::heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use meilisearch_types::milli::update::IndexerConfig;
use meilisearch_types::milli::Index;
use uuid::Uuid;

use self::IndexStatus::{Available, BeingDeleted};
use crate::{Error, Result};

const INDEX_MAPPING: &str = "index-mapping";

#[derive(Clone)]
pub struct IndexMapper {
    // Keep track of the opened indexes and is used
    // mainly by the index resolver.
    index_map: Arc<RwLock<HashMap<Uuid, IndexStatus>>>,

    // TODO create a UUID Codec that uses the 16 bytes representation
    // Map an index name with an index uuid currently available on disk.
    index_mapping: Database<Str, SerdeBincode<Uuid>>,

    base_path: PathBuf,
    index_size: usize,
    indexer_config: Arc<IndexerConfig>,
}

/// Weither the index must not be inserted back
/// or it is available for use.
#[derive(Clone)]
pub enum IndexStatus {
    /// Do not insert it back in the index map as it is currently being deleted.
    BeingDeleted,
    /// You can use the index without worrying about anything.
    Available(Index),
}

impl IndexMapper {
    pub fn new(
        env: &Env,
        base_path: PathBuf,
        index_size: usize,
        indexer_config: IndexerConfig,
    ) -> Result<Self> {
        Ok(Self {
            index_map: Arc::default(),
            index_mapping: env.create_database(Some(INDEX_MAPPING))?,
            base_path,
            index_size,
            indexer_config: Arc::new(indexer_config),
        })
    }

    /// Get or create the index.
    pub fn create_index(&self, wtxn: &mut RwTxn, name: &str) -> Result<Index> {
        match self.index(wtxn, name) {
            Ok(index) => Ok(index),
            Err(Error::IndexNotFound(_)) => {
                let uuid = Uuid::new_v4();
                self.index_mapping.put(wtxn, name, &uuid)?;

                let index_path = self.base_path.join(uuid.to_string());
                fs::create_dir_all(&index_path)?;
                let mut options = EnvOpenOptions::new();
                options.map_size(self.index_size);
                Ok(Index::new(options, &index_path)?)
            }
            error => error,
        }
    }

    /// Removes the index from the mapping table and the in-memory index map
    /// but keeps the associated tasks.
    pub fn delete_index(&self, wtxn: &mut RwTxn, name: &str) -> Result<()> {
        let uuid = self
            .index_mapping
            .get(&wtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // Once we retrieved the UUID of the index we remove it from the mapping table.
        assert!(self.index_mapping.delete(wtxn, name)?);

        // We remove the index from the in-memory index map.
        let mut lock = self.index_map.write().unwrap();
        let closing_event = match lock.insert(uuid, BeingDeleted) {
            Some(Available(index)) => Some(index.prepare_for_closing()),
            _ => None,
        };

        drop(lock);

        let index_map = self.index_map.clone();
        let index_path = self.base_path.join(uuid.to_string());
        let index_name = name.to_string();
        thread::spawn(move || {
            // We first wait to be sure that the previously opened index is effectively closed.
            // This can take a lot of time, this is why we do that in a seperate thread.
            if let Some(closing_event) = closing_event {
                closing_event.wait();
            }

            // Then we remove the content from disk.
            if let Err(e) = fs::remove_dir_all(&index_path) {
                error!(
                    "An error happened when deleting the index {} ({}): {}",
                    index_name, uuid, e
                );
            }

            // Finally we remove the entry from the index map.
            assert!(matches!(
                index_map.write().unwrap().remove(&uuid),
                Some(BeingDeleted)
            ));
        });

        Ok(())
    }

    /// Return an index, may open it if it wasn't already opened.
    pub fn index(&self, rtxn: &RoTxn, name: &str) -> Result<Index> {
        let uuid = self
            .index_mapping
            .get(rtxn, name)?
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;

        // we clone here to drop the lock before entering the match
        let index = self.index_map.read().unwrap().get(&uuid).cloned();
        let index = match index {
            Some(Available(index)) => index,
            Some(BeingDeleted) => return Err(Error::IndexNotFound(name.to_string())),
            // since we're lazy, it's possible that the index has not been opened yet.
            None => {
                let mut index_map = self.index_map.write().unwrap();
                // between the read lock and the write lock it's not impossible
                // that someone already opened the index (eg if two search happens
                // at the same time), thus before opening it we check a second time
                // if it's not already there.
                // Since there is a good chance it's not already there we can use
                // the entry method.
                match index_map.entry(uuid) {
                    Entry::Vacant(entry) => {
                        let index_path = self.base_path.join(uuid.to_string());
                        fs::create_dir_all(&index_path)?;
                        let mut options = EnvOpenOptions::new();
                        options.map_size(self.index_size);
                        let index = Index::new(options, &index_path)?;
                        entry.insert(Available(index.clone()));
                        index
                    }
                    Entry::Occupied(entry) => match entry.get() {
                        Available(index) => index.clone(),
                        BeingDeleted => return Err(Error::IndexNotFound(name.to_string())),
                    },
                }
            }
        };

        Ok(index)
    }

    pub fn indexes(&self, rtxn: &RoTxn) -> Result<Vec<(String, Index)>> {
        self.index_mapping
            .iter(rtxn)?
            .map(|ret| {
                ret.map_err(Error::from).and_then(|(name, _)| {
                    self.index(rtxn, name)
                        .map(|index| (name.to_string(), index))
                })
            })
            .collect()
    }

    /// Swap two index names.
    pub fn swap(&self, wtxn: &mut RwTxn, lhs: &str, rhs: &str) -> Result<()> {
        let lhs_uuid = self
            .index_mapping
            .get(wtxn, lhs)?
            .ok_or_else(|| Error::IndexNotFound(lhs.to_string()))?;
        let rhs_uuid = self
            .index_mapping
            .get(wtxn, rhs)?
            .ok_or_else(|| Error::IndexNotFound(rhs.to_string()))?;

        self.index_mapping.put(wtxn, lhs, &rhs_uuid)?;
        self.index_mapping.put(wtxn, rhs, &lhs_uuid)?;

        Ok(())
    }

    pub fn index_exists(&self, rtxn: &RoTxn, name: &str) -> Result<bool> {
        Ok(self.index_mapping.get(rtxn, name)?.is_some())
    }

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.indexer_config
    }
}
