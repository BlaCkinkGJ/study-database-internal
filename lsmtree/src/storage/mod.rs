use std::{
    fs::{read_dir, remove_file},
    io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{memtable::MemTable, wal::WAL};
use anyhow::{bail, Result};

pub struct Storage {
    memtable: MemTable,
    wal: WAL,
}

pub fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }

    files
}

pub fn load_from_dir(dir: &Path) -> io::Result<(WAL, MemTable)> {
    let mut wal_files = files_with_ext(dir, "wal");
    wal_files.sort();

    let mut new_mem_table = MemTable::new();
    let mut new_wal = WAL::new(dir)?;
    for wal_file in wal_files.iter() {
        if let Ok(wal) = WAL::from_path(wal_file) {
            for entry in wal.into_iter() {
                if entry.deleted {
                    new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
                    new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                } else {
                    new_mem_table.set(
                        entry.key.as_slice(),
                        entry.value.as_ref().unwrap().as_slice(),
                        entry.timestamp,
                    );
                    new_wal.set(
                        entry.key.as_slice(),
                        entry.value.unwrap().as_slice(),
                        entry.timestamp,
                    )?;
                }
            }
        }
    }
    new_wal.flush().unwrap();
    wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

    Ok((new_wal, new_mem_table))
}

impl Storage {
    pub fn new(dir: &Path) -> Result<Storage> {
        let (wal, memtable) = load_from_dir(dir)?;
        Ok(Storage { memtable, wal })
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
        self.wal.set(key, value, timestamp.as_millis())?;
        Ok(self.memtable.set(key, value, timestamp.as_millis()))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("invalid timestamp detected");
        self.wal.delete(key, timestamp.as_millis())?;
        Ok(self.memtable.delete(key, timestamp.as_millis()))
    }
}
