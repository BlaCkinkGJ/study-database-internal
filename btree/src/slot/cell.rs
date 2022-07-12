#[repr(C)]
pub struct Offset {
    payload_size: u64,
    start_cell_pos: u64,
}

#[repr(u8)]
enum CellType {
    Data = 0,
    Pointer = 1,
}

#[repr(C)]
pub struct Cell {
    cell_type: CellType,
    payload: Vec<u8>,
    next_cell_pos: u64,
}
