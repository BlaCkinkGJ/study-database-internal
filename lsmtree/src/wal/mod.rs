use std::{
    fs::{File, OpenOptions, remove_file},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct WalEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    pub fn new(path: PathBuf) -> io::Result<WalIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WalIterator { reader })
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
        let mut buffer= vec![0; length];
        self.reader.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn read_wal_entry(&mut self) -> io::Result<WalEntry> {
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
        Ok(WalEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

impl Iterator for WalIterator {
    type Item = WalEntry;

    fn next(&mut self) -> Option<WalEntry> {
        match self.read_wal_entry() {
            Ok(entry) => { Some(entry) },
            Err(_) => {None},
        }
    }
}

#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
}

impl Clone for Wal {
    fn clone(&self) -> Self {
        Self::from_path(&self.path).unwrap()
    }
}

impl Wal {
    pub fn new(dir: &Path) -> io::Result<Wal> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(Wal { path, file })
    }

    pub fn from_path(path: &Path) -> io::Result<Wal> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(Wal {
            path: path.to_owned(),
            file,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn drop(&mut self) -> io::Result<()> {
        remove_file(&self.path)
    }
}

impl IntoIterator for Wal {
    type IntoIter = WalIterator;
    type Item = WalEntry;

    fn into_iter(self) -> WalIterator {
        WalIterator::new(self.path).unwrap()
    }
}
