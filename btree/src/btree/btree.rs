use super::node::Node;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Btree {
    pub t: usize, // minimum degree
    pub root: Option<Box<Node>>,
}

impl Btree {
    pub fn search(&self, x: &'static Node, k: u64) -> Result<(&'static Node, usize)> {
        let mut i: usize = 0;
        while i < x.n && k > x.key[i as usize] {
            i = i + 1;
        }

        if i <= x.n && k == x.key[i as usize] {
            Ok((x, i))
        } else if x.leaf {
            bail!("cannot find the key({})", k)
        } else {
            match &x.c[i as usize] {
                Some(node) => self.search(&node, k),
                None => bail!("cannot find the key({})", k),
            }
        }
    }

    pub fn create(t: usize) -> Self {
        let x = Node::alloc_node(t);
        Self { t, root: Some(x) }
    }

    fn split_child(&self, x: &mut Node, i: usize) {
        let t = self.t;
        let mut z = Node::alloc_node(t);
        let y: &mut Box<Node> = match &mut x.c[i] {
            Some(node) => node,
            None => panic!("None value detected"),
        };
        z.leaf = y.leaf;
        z.n = t - 1;
        for j in 1..t {
            z.key[j] = y.key[j + t];
        }

        if !y.leaf {
            for j in 1..(t + 1) {
                z.c[j] = y.c[j + t].clone();
            }
        }
        y.n = t - 1;
        let ykey = y.key[t];
        for j in ((i + 1)..(x.n + 2)).rev() {
            x.c[j + 1] = match &x.c[j] {
                Some(node) => Some(node.clone()),
                None => None,
            };
        }
        x.c[i + 1] = Some(z);
        for j in (i..(x.n + 1)).rev() {
            x.key[j + 1] = x.key[j];
        }
        x.key[i] = ykey;
        x.n = x.n + 1;
    }
}
