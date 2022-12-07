pub struct MemTableMeta {
    pub timestamp: u128,
    pub deleted: bool,
}

pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub meta: MemTableMeta,
}

pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: isize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            entries: Vec::new(),
            size: 0,
        }
    }

    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
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
                    self.size += value.len() as isize - old.len() as isize
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size +=
                    (key.len() + value.len() + std::mem::size_of::<MemTableMeta>()) as isize;
                self.entries.insert(idx, entry)
            }
        }
    }

    // Add tombstone to memtable
    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
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
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entries[idx]);
        }
        None
    }
}
