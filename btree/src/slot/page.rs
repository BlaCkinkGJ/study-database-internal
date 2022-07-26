use super::cell::{Cell, Offset};
use anyhow::{bail, Result};
use std::{
    fs::File,
    intrinsics::copy,
    io::{Read, Seek, SeekFrom, Write},
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

    pub fn try_add_offset_cursor(&self, offset_size: usize) -> Result<u64> {
        let new_offset_cursor = self.offset_cursor + offset_size as u64;

        if new_offset_cursor + self.cell_cursor > self.total_body_size {
            bail!(
                "cursor overflow detected (offset: {}, cell: {})",
                new_offset_cursor,
                self.cell_cursor
            )
        }

        Ok(new_offset_cursor)
    }

    pub fn add_offset_cursor(&mut self, offset_size: usize) -> Result<u64> {
        let new_offset_cursor = self.try_add_offset_cursor(offset_size)?;
        self.offset_cursor = new_offset_cursor;

        Ok(new_offset_cursor)
    }

    pub fn try_add_cell_cursor(&self, cell_size: usize) -> Result<u64> {
        let new_cell_cursor = self.cell_cursor + cell_size as u64;

        if self.offset_cursor + new_cell_cursor > self.total_body_size {
            bail!(
                "cursor overflow detected (offset: {}, cell: {})",
                new_cell_cursor,
                self.cell_cursor
            )
        }

        Ok(new_cell_cursor)
    }

    pub fn add_cell_cursor(&mut self, cell_size: usize) -> Result<u64> {
        let new_cell_cursor = self.try_add_cell_cursor(cell_size)?;
        self.cell_cursor = new_cell_cursor;

        Ok(new_cell_cursor)
    }
}

const PAGE_SIZE: usize = 4096; // bytes

#[repr(C)]
pub struct SlottedPage {
    header: SlottedHeader,
    body: [u8; PAGE_SIZE],
}

impl SlottedPage {
    pub fn new() -> Self {
        let header = SlottedHeader::new(PAGE_SIZE as u64);
        Self {
            header,
            body: [0; PAGE_SIZE],
        }
    }

    fn get_serialized_cell_size(payload: &Vec<u8>) -> Result<usize> {
        match bincode::serialized_size(&Cell {
            cell_size: 0 as u64,
            next_cell_pos: 0 as u64,
            payload: payload.clone(),
        }) {
            Ok(cell_size) => Ok(cell_size as usize),
            Err(e) => bail!(e),
        }
    }

    fn write_cell_to_body(&mut self, payload: &Vec<u8>) -> Result<(usize, usize)> {
        let cell_size = Self::get_serialized_cell_size(payload)?;
        let cell = bincode::serialize(&mut Cell {
            cell_size: cell_size as u64,
            next_cell_pos: 0 as u64,
            payload: payload.clone(),
        })?;
        if let Err(e) = self.header.try_add_cell_cursor(cell_size) {
            bail!("add payload failed: {:.?}", e);
        }

        // cell writes from the back
        let to = (self.header.total_body_size - self.header.cell_cursor) as usize;
        let from = to - cell.len(); // cell_start_pos
        self.body[from..to].clone_from_slice(&cell);
        Ok((from, cell_size))
    }

    fn write_offset_to_body(&mut self, payload: &Vec<u8>, cell_start_pos: usize) -> Result<usize> {
        let offset = bincode::serialize(&Offset {
            payload_size: payload.len() as u64,
            start_cell_pos: cell_start_pos as u64,
        })?;

        let offset_size = offset.len();
        if let Err(e) = self.header.try_add_offset_cursor(offset_size) {
            bail!("add payload failed: {:.?}", e);
        }

        // offset writes from the start
        let from = self.header.offset_cursor as usize;
        let to = from + offset.len() as usize;
        self.body[from..to].clone_from_slice(&offset);

        Ok(offset_size)
    }

    pub fn add_payload(&mut self, payload: &Vec<u8>) -> Result<()> {
        // write payload to body
        let (start_cell_pos, cell_size) = self.write_cell_to_body(payload)?;
        let offset_size = self.write_offset_to_body(payload, start_cell_pos)?;

        // update metadata
        self.header.add_offset_cursor(offset_size)?;
        self.header.add_cell_cursor(cell_size)?;

        Ok(())
    }

    fn read_offset_from_body(&self, offset_index: usize) -> Result<Offset> {
        let offset_cursor = (offset_index * size_of::<Offset>()) as u64;
        if offset_cursor > self.header.offset_cursor {
            bail!(
                "overflow offset cursor ({:.?} > {:.?})",
                offset_cursor,
                self.header.offset_cursor
            )
        }
        let from = offset_cursor as usize;
        let to = from + size_of::<Offset>();
        let mut buffer: Vec<u8> = vec![0; size_of::<Offset>()];
        buffer.clone_from_slice(&self.body[from..to]);

        let offset = bincode::deserialize::<Offset>(buffer.as_slice())?;
        Ok(offset)
    }

    fn get_cell_size_from_buffer(&self, offset: &Offset) -> usize {
        let mut buffer: Vec<u8> = vec![0; size_of::<u64>()];
        let mut cell_size: [u8; 8] = [0; 8];

        let from = offset.start_cell_pos as usize;
        let to = from + size_of::<u64>(); // size of "cell_size"
        buffer.clone_from_slice(&self.body[from..to]);
        cell_size.clone_from_slice(buffer.as_slice());

        let cell_size = u64::from_le_bytes(cell_size) as usize;
        cell_size
    }

    fn read_cell_from_body(&self, offset: &Offset) -> Result<Cell> {
        let cell_size = self.get_cell_size_from_buffer(offset);
        let from = offset.start_cell_pos as usize;
        let to = from + cell_size;
        let mut buffer: Vec<u8> = vec![0; cell_size];
        buffer.clone_from_slice(&self.body[from..to]);
        let cell = bincode::deserialize::<Cell>(buffer.as_slice())?;
        Ok(cell)
    }

    pub fn read_payload(&self, offset_index: usize) -> Result<Vec<u8>> {
        let offset = self.read_offset_from_body(offset_index)?;
        let cell = self.read_cell_from_body(&offset)?;
        Ok(cell.payload)
    }

    fn serialize_struct<'a, T>(target: *const T) -> &'a [u8] {
        let ptr: *const u8 = target as *const u8;
        let slice: &[u8] = unsafe { slice::from_raw_parts(ptr, size_of::<T>()) };
        slice
    }

    pub fn pack(page: &Self, file: &mut File, pos: u64) -> Result<usize> {
        let ptr: *const Self = page;
        let slice = Self::serialize_struct(ptr);
        file.seek(SeekFrom::Start(pos))?;
        match file.write(slice) {
            Ok(write_size) => {
                file.flush().expect("fail to flush file");
                Ok(write_size)
            }
            Err(e) => bail!(e),
        }
    }

    fn serialize_mut_struct<'a, T>(target: *mut T) -> &'a mut [u8] {
        let slice: &mut [u8];
        unsafe {
            let len = size_of::<T>();
            slice = slice::from_raw_parts_mut(target as *mut _ as *mut u8, len);
        }
        slice
    }

    pub fn unpack(file: &mut File, pos: u64) -> Result<Self> {
        let mut page: Self = unsafe { zeroed() };
        file.seek(SeekFrom::Start(pos))?;
        let slice = Self::serialize_mut_struct(&mut page);
        file.read_exact(slice)?;

        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{bail, Result};
    use rand::{thread_rng, Rng};
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

        let mut pos_vec: Vec<usize> = vec![];
        let mut write_pos: usize = 0;
        let size = 10;
        (0..100).for_each(|i: usize| {
            let mut page = SlottedPage::new();
            for j in 0..size {
                let index = (size * i) + j;
                page.add_payload(&index.to_le_bytes().to_vec())
                    .expect("add payload failed");
            }
            let write_size =
                SlottedPage::pack(&page, &mut file, write_pos as u64).expect("fail to pack");
            pos_vec.push(write_pos);
            write_pos += write_size;
        });

        let mut file = match OpenOptions::new().read(true).open(filename.clone()) {
            Ok(file) => file,
            Err(e) => bail!("{:.?} fails to open: {:.?})", filename, e),
        };

        (0..100).rev().for_each(|i: usize| {
            let pos = match pos_vec.pop() {
                Some(pos) => pos,
                None => panic!("position corruption detected"),
            };
            let page = SlottedPage::unpack(&mut file, pos as u64).expect("fail to unpack");
            assert_eq!(magic, page.header.magic);
            for j in 0..size {
                let index = (size * i) + j;
                let mut payload: [u8; 8] = [0; 8];
                payload.clone_from_slice(&page.read_payload(j).expect("add payload failed"));
                assert_eq!(index, usize::from_le_bytes(payload));
            }
        });

        Ok(())
    }

    #[test]
    fn serialize_happy() {
        let tempfile = NamedTempFile::new().expect("fail to create temporal file");
        if let Err(e) = page_serialize_happy(tempfile.as_ref()) {
            eprintln!("page serialization failed: {:.?}", e);
        }
        if let Err(e) = tempfile.close() {
            eprintln!("fail to close temporary file: {:.?}", e);
        }
    }

    fn is_same_payload(a: &Vec<u8>, b: &Vec<u8>) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }

    #[test]
    fn add_payload_happy() {
        let mut rng = thread_rng();
        let mut page = SlottedPage::new();
        let test_cases: Vec<Vec<u8>> = (0..5)
            .map(|_| {
                let size: usize = rng.gen_range(1..4096);
                (0..size).map(|_| rng.gen::<u8>()).collect()
            })
            .collect();
        let (mut i, mut j) = (0, 0);
        test_cases.iter().for_each(|data: &Vec<u8>| {
            while let Err(_) = page.add_payload(data) {
                page = SlottedPage::new();
                i = 0;
            }
            let payload = page.read_payload(i).expect("fail to read payload");
            assert_eq!(
                true,
                is_same_payload(&test_cases[j], &payload),
                "{:.?} != {:.?}",
                test_cases[j].len(),
                payload.len()
            );
            i += 1;
            j += 1;
        });
    }
}
