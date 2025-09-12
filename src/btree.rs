mod page;

use crate::bufferPool::BufferPool;
use page::PageId;
use std::io;

#[derive(Debug)]
pub struct BTree {
    root_page_id: Option<PageId>,
}

#[derive(Debug, Default)]
pub struct Node {
    keys: Vec<PageId>,
    children: Vec<PageId>,
    is_leaf: bool
}

impl BTree {
    fn new() -> Self {
        BTree {
            root_page_id: None,
        }
    }

    fn insert(&mut self, key: PageId, buffer_pool: &mut BufferPool) -> io::Result<()> {
        match self.root_page_id {
            None => {
                // Create first root
                let root_node = Node {
                    keys: vec![key],
                    children: Vec::new(),
                    is_leaf: true,
                };
                let root_page_id = self.allocate_node(root_node, buffer_pool)?;
                self.root_page_id = Some(root_page_id);
            }
            Some(root_id) => {
                let root_node = self.load_node(root_id, buffer_pool)?;
                if root_node.keys.len() == 2 * T - 1 {
                    // Split root
                    let new_root = Node {
                        keys: Vec::new(),
                        children: vec![root_id],
                        is_leaf: false,
                    };
                    let new_root_id = self.allocate_node(new_root, buffer_pool)?;
                    self.split_child(new_root_id, 0, buffer_pool)?;
                    self.insert_non_full(new_root_id, key, buffer_pool)?;
                    self.root_page_id = Some(new_root_id);
                } else {
                    self.insert_non_full(root_id, key, buffer_pool)?;
                }
            }
        }
    }

    fn search(&self, key: PageId, buffer_pool: &mut BufferPool) -> io::Result<bool> {
        match self.root_page_id {
            None => Ok(false),
            Some(root_id) => self.search_node(root_id, key, buffer_pool),
        }
    }
}
impl Node {
    // Serialize node to page data
    fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut data = [0u8; PAGE_SIZE];
        let mut offset = 0;

        // Write is_leaf flag
        data[offset] = if self.is_leaf { 1 } else { 0 };
        offset += 1;

        // Write number of keys
        let key_count = self.keys.len() as u32;
        data[offset..offset + 4].copy_from_slice(&key_count.to_le_bytes());
        offset += 4;

        // Write keys
        for key in &self.keys {
            data[offset..offset + 8].copy_from_slice(&key.0.to_le_bytes());
            offset += 8;
        }

        // Write number of children
        let child_count = self.children.len() as u32;
        data[offset..offset + 4].copy_from_slice(&child.count.to_le_bytes());
        offset += 4;

        // Write children
        for child in &self.children {
            data[offset..offset + 8].copy_from_slice(&child.0.to_le_bytes());
            offset += 8;
        }

        data
    }

    // Deserialize node from page data
    fn deserialize(data: &[u8; PAGE_SIZE]) -> Self {
        let mut offset = 0;

        // Read is_leaf flag
        let is_leaf = data[offset] == 1;
        offset += 1;

        // Read number of keys
        let key_count = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;

        // Read keys
        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            let key_val = u64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
            ]);
            keys.push(PageId(key_val));
            offset += 8;
        }

        // Read number of children
        let child_count = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        
        // Read children
        let mut children = Vec::with_capacity(child_count);
        for _ in 0..child_count {
            let child_val = u64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
            ]);
            children.push(PageId(child_val));
            offset += 8;
        }
        
        Node { keys, children, is_leaf }

    }
}







