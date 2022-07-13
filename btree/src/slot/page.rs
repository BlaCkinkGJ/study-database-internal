use super::cell::{Cell, Offset};
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
    total_body_size: u64,
}

const MAGIC_STRING: *const [u8; 7] = "btree\0\0".as_bytes().as_ptr() as *const [u8; 7];

impl SlottedHeader {
    pub fn new(total_body_size: u64) -> Self {
        Self {
            magic: unsafe { *MAGIC_STRING },
            reserved: unsafe { zeroed() },
            offset_cursor: 0 as u64,
            cell_cursor: 0 as u64,
            total_body_size,
        }
    }

    pub fn add_offset_cursor(&mut self, offset_size: u64) -> Result<u64> {
        let new_offset_cursor = self.offset_cursor + offset_size;

        if new_offset_cursor + self.cell_cursor > self.total_body_size {
            bail!(
                "cursor overflow detected (offset: {}, cell: {})",
                new_offset_cursor,
                self.cell_cursor
            )
        }

        self.offset_cursor = new_offset_cursor;

        Ok(new_offset_cursor)
    }

    pub fn add_cell_cursor(&mut self, cell_size: u64) -> Result<u64> {
        let new_cell_cursor = self.cell_cursor + cell_size;

        if self.offset_cursor + new_cell_cursor > self.total_body_size {
            bail!(
                "cursor overflow detected (offset: {}, cell: {})",
                new_cell_cursor,
                self.cell_cursor
            )
        }

        self.cell_cursor = new_cell_cursor;

        Ok(new_cell_cursor)
    }
}

const PAGE_BODY_SIZE: usize = 264;

#[repr(C)]
pub struct SlottedPage {
    header: SlottedHeader,
    body: [u8; PAGE_BODY_SIZE],
}

impl SlottedPage {
    pub fn new() -> Self {
        let header = SlottedHeader::new(PAGE_BODY_SIZE as u64);
        Self {
            header,
            body: unsafe { zeroed() },
        }
    }

    pub fn add_payload(&self, payload: &Vec<u8>) -> Result<()> {
        let offset = &Offset {
            payload_size: payload.len() as u64,
            start_cell_pos: self.header.cell_cursor,
        };
        let cell = &Cell {
            payload: payload.clone(),
            next_cell_pos: 0,
        };
        Ok(())
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

        (0..100).for_each(|_: u64| {
            let page = SlottedPage::new();
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
            assert_eq!(PAGE_BODY_SIZE as u64, page.header.total_body_size);
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
