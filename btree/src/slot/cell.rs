#[repr(C)]
pub struct Offset {
    payload_size: u64,
    start_cell_pos: u64,
}

#[repr(C)]
pub struct Cell {
    payload: Vec<u8>,
    next_cell_pos: u64,
}
