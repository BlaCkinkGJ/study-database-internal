use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Offset {
    payload_size: u64,
    start_cell_pos: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Cell {
    payload: Vec<u8>,
    next_cell_pos: u64,
}
