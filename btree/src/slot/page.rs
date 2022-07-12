use super::cell::Cell;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::Read;
use std::{
    io::{Seek, SeekFrom, Write},
    mem::{size_of, zeroed},
    slice,
};

#[repr(C)]
pub struct SlottedHeader {
    magic: [u8; 7],
    reserved: u8,
    offset_cursor: u64,
    cell_cursor: u64,
}

const MAGIC_STRING: *const [u8; 7] = "btree\0\0".as_bytes().as_ptr() as *const [u8; 7];

impl SlottedHeader {
    pub fn new(offset_cursor: u64, cell_cursor: u64) -> Self {
        Self {
            magic: unsafe { *MAGIC_STRING },
            reserved: unsafe { zeroed() },
            offset_cursor,
            cell_cursor,
        }
    }
}

#[repr(C)]
pub struct SlottedPage {
    pub header: SlottedHeader,
    pub body: [u8; 264],
}

impl SlottedPage {
    pub fn new(header: SlottedHeader) -> Self {
        Self {
            header,
            body: unsafe { zeroed() },
        }
    }

    pub fn pack(page: &Self, file: &mut File, pos: u64) -> Result<usize> {
        let ptr: *const Self = page;
        let ptr: *const u8 = ptr as *const u8;
        let slice: &[u8] = unsafe { slice::from_raw_parts(ptr, size_of::<Self>()) };
        file.seek(SeekFrom::Start(pos)).expect("fail to seek file");
        match file.write(slice) {
            Ok(write_size) => Ok(write_size),
            Err(e) => bail!(e),
        }
    }

    pub fn unpack(file: &mut File, pos: u64) -> Result<Self> {
        let mut page: Self = unsafe { zeroed() };
        file.seek(SeekFrom::Start(pos)).expect("fail to seek file");
        unsafe {
            let len = size_of::<Self>();
            let slice = slice::from_raw_parts_mut(&mut page as *mut _ as *mut u8, len);
            file.read_exact(slice).expect("fail to read file");
        }

        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{bail, Result};
    use std::{fs::OpenOptions, mem::size_of, path::Path};
    use tempfile::NamedTempFile;

    fn page_serialize_happy(tempfile: &Path) -> Result<()> {
        let filename = match tempfile.to_str() {
            Some(filename) => filename,
            None => bail!("fail to get string type of filename"),
        };

        println!("Temporary File: {:.?}", filename);

        let magic = unsafe { *MAGIC_STRING };

        let mut file = match OpenOptions::new()
            .write(true)
            .append(true)
            .open(filename.clone())
        {
            Ok(file) => file,
            Err(e) => bail!("{:.?} fails to open: {:.?})", filename, e),
        };

        (0..100).for_each(|i: u64| {
            let page = SlottedPage::new(SlottedHeader::new(i, i + 100));
            let write_size = SlottedPage::pack(&page, &mut file, 0).expect("file write failed");
            assert_eq!(size_of::<SlottedPage>(), write_size);
        });

        let mut file = match OpenOptions::new().read(true).open(filename.clone()) {
            Ok(file) => file,
            Err(e) => bail!("{:.?} fails to open: {:.?})", filename, e),
        };

        (0..100).rev().for_each(|i: u64| {
            let pos = (size_of::<SlottedPage>() as u64) * (i as u64);
            let page = SlottedPage::unpack(&mut file, pos).expect("fail to unpack");
            assert_eq!(magic, page.header.magic);
            assert_eq!(i, page.header.offset_cursor);
            assert_eq!(i + 100, page.header.cell_cursor);
        });

        Ok(())
    }

    #[test]
    fn serialize_happy() {
        let tempfile = NamedTempFile::new().expect("create temporary file failed");
        if let Err(e) = page_serialize_happy(tempfile.as_ref()) {
            eprintln!("page serialization failed: {:.?}", e);
        }
        if let Err(e) = tempfile.close() {
            eprintln!("fail to close temporary file: {:.?}", e);
        }
    }
}
