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
            key: vec![0; 2 * t as usize],
            c: vec![None; 2 * (t - 1) as usize],
        })
    }

    pub fn clone(node: &Self) -> Box<Self> {
        Box::new(Node {
            n: node.n,
            leaf: node.leaf,
            key: node.key.clone(),
            c: node.c.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_node_happy() {
        let node = Node::alloc_node(5);
        for j in (1..10).rev() {
            println!("{}", j);
        }
    }
}
