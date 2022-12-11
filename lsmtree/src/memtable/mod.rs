use std::{path::{Path, PathBuf}, fs::{read_dir, remove_file}};

use anyhow::{Result, bail};

use crate::wal::Wal;

#[derive(Clone, Debug)]
pub struct MemTableMeta {
    pub timestamp: u128,
    pub deleted: bool,
}

#[derive(Clone, Debug)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub meta: MemTableMeta,
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

#[derive(Clone, Debug)]
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: isize,
    wal: Wal,
    is_immutable: bool,
}

impl MemTable {
    pub fn new(dir: &Path) -> Result<MemTable> {
        println!("create!");
        Ok(MemTable {
            entries: Vec::new(),
            size: 0,
            wal: Wal::new(dir)?,
            is_immutable: false,
        })
    }


    pub fn load_from_dir(dir: &Path) -> Result<Self> {
        let mut wal_files = files_with_ext(dir, "wal");
        wal_files.sort();

        let mut new_mem_table = MemTable::new(dir)?;
        for wal_file in wal_files.iter() {
            if let Ok(wal) = Wal::from_path(wal_file) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_mem_table.delete(entry.key.as_slice(), entry.timestamp)?;
                        new_mem_table.wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    } else {
                        new_mem_table.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        )?;
                        new_mem_table.wal.set(
                            entry.key.as_slice(),
                            entry.value.unwrap().as_slice(),
                            entry.timestamp,
                        )?;
                    }
                }
            }
        }
        new_mem_table.wal.flush().unwrap();
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

        Ok(new_mem_table)
    }

    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> Result<()> {
        if self.is_immutable {
            bail!("do not try to write immutable memory table")
        }
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            meta: MemTableMeta {
                timestamp,
                deleted: false,
            },
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(old) = self.entries[idx].value.as_ref() {
                    self.size += value.len() as isize - old.len() as isize;
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size +=
                    (key.len() + value.len() + std::mem::size_of::<MemTableMeta>()) as isize;
                self.entries.insert(idx, entry);
            }
        }
        self.wal.set(key, value, timestamp)?;
        Ok(())
    }

    // Add tombstone to memtable
    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> Result<()> {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            meta: MemTableMeta {
                timestamp,
                deleted: true,
            },
        };
        match self.get_index(key) {
            Ok(idx) => {
                if let Some(old) = self.entries[idx].value.as_ref() {
                    self.size -= old.len() as isize;
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size += (key.len() + std::mem::size_of::<MemTableMeta>()) as isize;
                self.entries.insert(idx, entry)
            }
        }
        self.wal.delete(key, timestamp)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }
        None
    }

    pub fn size(&self) -> isize {
        self.size
    }

    pub fn to_immutable(&mut self) {
        self.is_immutable = true;
    }

    pub fn drop(&mut self) -> Result<()> {
        self.wal.drop()?;
        Ok(())
    }
}