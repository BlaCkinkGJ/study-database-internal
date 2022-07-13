#[repr(C)]
pub struct Offset {
    pub payload_size: u64,
    pub start_cell_pos: u64,
}

#[repr(C)]
pub struct Cell {
    pub cell_size: u64,
    pub next_cell_pos: u64,
    pub payload: Vec<u8>,
}
