use serde::{Deserialize, Serialize};

// Do not add variable length field in this structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Offset {
    pub payload_size: u64,
    pub start_cell_pos: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Cell {
    pub cell_size: u64,
    pub next_cell_pos: u64,
    pub payload: Vec<u8>,
}
