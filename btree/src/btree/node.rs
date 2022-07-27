use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Node {
    pub n: usize,
    pub leaf: bool,
    pub key: Vec<u64>,
    pub c: Vec<Option<Box<Node>>>,
}

impl Node {
    pub fn alloc_node(t: usize) -> Box<Self> {
        Box::new(Self {
            n: 0,
            leaf: true,
            key: vec![0; 2 * t - 1 as usize],
            c: vec![None; 2 * t as usize],
        })
    }
}
