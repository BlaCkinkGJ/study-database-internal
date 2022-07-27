use super::node::Node;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Btree {
    pub t: usize, // minimum degree
    pub root: Option<Box<Node>>,
}

impl Btree {
    pub fn search(&self, x: &'static Box<Node>, k: u64) -> Result<(&'static Box<Node>, usize)> {
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

    fn split_child(x: &mut Box<Node>, i: usize, t: usize) -> Result<()> {
        let mut z = Node::alloc_node(t);
        let y = match &mut x.c[i] {
            Some(node) => node,
            None => bail!("None value detected"),
        };
        z.leaf = y.leaf;
        z.n = t - 1;
        for j in 0..(t - 1) {
            z.key[j] = y.key[j + t];
        }

        if !y.leaf {
            for j in 0..t {
                z.c[j] = y.c[j + t].clone();
            }
        }
        y.n = t - 1;
        let ykey = y.key[t - 1];

        // update child
        for j in (i..(x.n + 1)).rev() {
            x.c[j + 1] = match &x.c[j] {
                Some(node) => Some(node.clone()),
                None => None,
            };
        }
        x.c[i] = Some(z);

        // update key
        for j in (i..(x.n + 1)).rev() {
            x.key[j] = x.key[j - 1];
        }
        x.key[i] = ykey;
        x.n = x.n + 1;
        Ok(())
    }

    fn insert(&mut self, k: u64) -> Result<()> {
        let r = match &self.root {
            Some(root) => root,
            None => bail!("None root detected"),
        };

        let t = self.t;
        if r.n == 2 * t - 1 {
            let mut s = Node::alloc_node(t);
            s.leaf = false;
            s.n = 0;
            s.c[0] = Some(r.clone());
            self.root = Some(s);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_node(t: usize, key: u64) -> Option<Box<Node>> {
        let mut node = Node::alloc_node(t);
        node.n = 2 * t;
        for i in 0..(2 * t - 1) {
            node.key[i] = key * (2 * t - 1) as u64 + i as u64;
        }
        Some(node)
    }
    #[test]
    fn box_test() {
        let mut test = Box::new(1);
        let test2 = &mut test;
        **test2 = 3;
        println!(">>>> {:.?}", test);
    }
    #[test]
    fn split_child_happy() {
        let t = 2;
        let mut btree = Btree::create(t);
        let mut node = Node::alloc_node(t);
        let child = vec![generate_node(t, 0), generate_node(t, 1)];
        node.n = 1;
        node.c = child.clone();
        node.leaf = false;
        btree.root = Some(node);
        println!("{:#?}", btree.root);
        if let Some(ref mut node) = btree.root {
            Btree::split_child(node, 1, btree.t).expect("something wrong happened");
        }
        println!("{:#?}", btree.root);
    }
}
