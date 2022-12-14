use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::memtable::MemTable;
use anyhow::{bail, Result};

// TODO: add multiplier for sstable
pub struct StorageConfig {
    pub threshold_bytes: isize,
    pub dir: PathBuf,
}

impl StorageConfig {
    pub fn default_config() -> Self {
        Self {
            threshold_bytes: 50,
            dir: Path::new("./tmp").to_path_buf(),
        }
    }
}

// TODO: implement SSTABLE based on Tiering Policy
pub struct Storage {
    mutable: MemTable,
    immutable: Vec<MemTable>,
    config: StorageConfig,
}

impl Storage {
    pub fn new(config: StorageConfig) -> Result<Self> {
        let mutable = MemTable::load_from_dir(&config.dir)?;
        let immutable = vec![];
        Ok(Self {
            mutable,
            immutable,
            config,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        // check memtable
        if let Some(entry) = self.mutable.get(key) {
            if let Some(value) = entry.value.as_ref() {
                return Ok(value.clone());
            }
        };

        // check immutable tables
        for memtable in self.immutable.iter() {
            if let Some(entry) = memtable.get(key) {
                if let Some(value) = entry.value.as_ref() {
                    return Ok(value.clone());
                }
            }
        }

        // fail to find
        bail!(format!("cannot find the key ==> {:?}", key))
    }

    pub fn create_immutable_if_exceed_threshold(&mut self) -> Result<()> {
        if self.mutable.size() >= self.config.threshold_bytes {
            self.mutable.to_immutable();
            // TODO: you must implement SSTABLE flush routine in here.
            self.immutable.push(self.mutable.clone());
            self.mutable = MemTable::new(&self.config.dir)?;
        }
        Ok(())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("invalid timestamp detected");
        self.mutable.set(key, value, timestamp.as_millis())?;
        self.create_immutable_if_exceed_threshold()?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("invalid timestamp detected");
        self.mutable.delete(key, timestamp.as_millis())?;
        self.create_immutable_if_exceed_threshold()?;
        Ok(())
    }

    pub fn drop(&mut self) -> Result<()> {
        self.mutable.drop()?;
        self.immutable
            .iter_mut()
            .for_each(|memtable| memtable.drop().unwrap());
        Ok(())
    }
}
