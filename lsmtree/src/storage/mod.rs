use std::time::{SystemTime, UNIX_EPOCH};

use crate::memtable::MemTable;
use anyhow::{bail, Result};

pub struct Storage {
    memtable: MemTable,
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            memtable: MemTable::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        match self.memtable.get(key) {
            Some(entry) => {
                if let Some(value) = entry.value.as_ref() {
                    Ok(value.clone())
                } else {
                    bail!(format!("cannot find the key ==> {:?}", key))
                }
            }
            None => {
                bail!(format!("cannot find the key ==> {:?}", key))
            }
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("invalid timestamp detected");
        Ok(self.memtable.set(key, value, timestamp.as_millis()))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("invalid timestamp detected");
        Ok(self.memtable.delete(key, timestamp.as_millis()))
    }
}
