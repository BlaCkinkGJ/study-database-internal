use std::{
    cmp,
    fs::{remove_file, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::ds::TableEntry;

pub struct SortedStringTableIterator {
    reader: BufReader<File>,
}

impl SortedStringTableIterator {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }

    fn read_usize(&mut self) -> io::Result<usize> {
        let mut buffer = [0; 8];
        self.reader.read_exact(&mut buffer)?;
        Ok(usize::from_le_bytes(buffer))
    }

    fn read_u128(&mut self) -> io::Result<u128> {
        let mut buffer = [0; 16];
        self.reader.read_exact(&mut buffer)?;
        Ok(u128::from_le_bytes(buffer))
    }

    fn read_bool(&mut self) -> io::Result<bool> {
        let mut bool_buffer = [0; 1];
        self.reader.read_exact(&mut bool_buffer)?;
        Ok(bool_buffer[0] != 0)
    }

    fn read_data(&mut self, length: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0; length];
        self.reader.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn read_wal_entry(&mut self) -> io::Result<TableEntry> {
        let key_len = self.read_usize()?;
        let deleted = self.read_bool()?;

        let (key, value) = if !deleted {
            let value_len = self.read_usize()?;

            let key = self.read_data(key_len)?;
            let value = self.read_data(value_len)?;

            (key, Some(value))
        } else {
            // read deleted entry
            (self.read_data(key_len)?, None)
        };
        let timestamp = self.read_u128()?;
        Ok(TableEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

impl Iterator for SortedStringTableIterator {
    type Item = TableEntry;

    fn next(&mut self) -> Option<TableEntry> {
        match self.read_wal_entry() {
            Ok(entry) => Some(entry),
            Err(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct SortedStringTable {
    path: PathBuf,
    file: BufWriter<File>,
}

impl SortedStringTable {
    pub fn new(dir: &Path) -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".sst");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(Self { path, file })
    }

    fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn compaction(old: Vec<TableEntry>, new: Vec<TableEntry>) -> io::Result<Vec<TableEntry>> {
        let mut old = old.clone();
        let mut new = new.clone();
        old.sort_by(|a, b| a.key.cmp(&b.key));
        new.sort_by(|a, b| a.key.cmp(&b.key));

        let mut merged = Vec::with_capacity(cmp::max(old.len(), new.len()));
        let (mut i, mut j) = (0, 0);
        loop {
            if i == old.len() {
                for j in i..new.len() {
                    merged.push(new[j].clone());
                }
                break;
            }
            if j == new.len() {
                for i in j..new.len() {
                    merged.push(old[i].clone());
                }
                break;
            }
            if old[i].key < new[j].key {
                merged.push(old[i].clone());
                i += 1;
            }
            if old[i].key > new[j].key {
                merged.push(new[j].clone());
                j += 1;
            }
            // TODO: add discarding existed
            if old[i].key == new[j].key {
                merged.push(new[j].clone());
                i += 1;
                j += 1;
            }
        }
        Ok(merged)
    }

    pub fn write(&mut self, entries: Vec<TableEntry>) -> io::Result<()> {
        for entry in entries {
            match &entry.value {
                Some(value) => {
                    if let Err(e) = self.set(&entry.key, &value, entry.timestamp) {
                        return Err(e);
                    }
                }
                None => {
                    if let Err(e) = self.delete(&entry.key, entry.timestamp) {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn drop(&mut self) -> io::Result<()> {
        remove_file(&self.path)
    }
}

impl IntoIterator for SortedStringTable {
    type IntoIter = SortedStringTableIterator;
    type Item = TableEntry;

    fn into_iter(self) -> SortedStringTableIterator {
        SortedStringTableIterator::new(self.path).unwrap()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn compaction_test() {}
}
