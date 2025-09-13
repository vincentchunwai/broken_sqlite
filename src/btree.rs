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

const T: usize = 3;

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

    fn load_node(&self, page_id: PageId, buffer_pool: &mut BufferPool) -> io::Result<Node> {
        let page = buffer_pool.get_page(page_id)?;
        Ok(Node::deserialize(&page.data))
    }

    fn save_node(&self, page_id: PageId, node: &Node, buffer_pool: &mut BufferPool) -> io::Result<()> {
        let page = buffer_pool.get_page(page_id)?;
        page.data = node.serialize();
        buffer_pool.mark_dirty(page_id);
        Ok(())
    }

    fn allocate_node(&self, node: Node, buffer_pool: &mut BufferPool) -> io::Result<PageId> {
        let page_id = buffer_pool.allocate_page();
        Ok(page_id)
    }

    fn search_node(&self, page_id: PageId, key: PageId, buffer_pool: &mut BufferPool) -> io::Result<bool> {
        let node = self.load_node(page_id, buffer_pool)?;

        let mut i = 0;
        while i < node.keys.len() && key > node.keys[i] {
            i += 1;
        }

        if i < node.keys.len() && key == node.keys[i] {
            return Ok(true);
        }

        if node.is_leaf {
            Ok(false)
        } else {
            self.search_node(node.children[i], key, buffer_pool)
        }
    }

    fn insert_non_full(&self, page_id: PageId, key: PageId, buffer_pool: &mut BufferPool) -> io::Result<()> {
        let mut node = self.load_node(page_id, buffer_pool)?;

        let mut i = node.keys.len();
        if node.is_leaf {
            while i > 0 && key < node.keys[i - 1] {
                i -= 1;
            }
            node.keys.insert(i, key);
            self.save_node(page_id, &node, buffer_pool)?;
        } else {
            while i > 0 && key < node.keys[i - 1] {
                i -= 1;
            }

            let child_id = node.children[i];
            let child_node = self.load_node(child_id, buffer_pool)?;

            if child_node.keys.len() == 2 * T - 1 {
                self.split_child(page_id, i, buffer_pool)?;
                let updated_node = self.load_node(page_id, buffer_pool)?;
                if key > updated_node.keys[i] {
                    i += 1;
                }
            }

            let updated_node = self.load_node(page_id, buffer_pool)?;
            self.insert_non_full(updated_node.children[i], key, buffer_pool)?;
        }
        Ok(())
    }

    fn split_child(&self, parent_id: PageId, child_index: usize, buffer_pool: &mut BufferPool) -> io::Result<()> {
        let mut parent = self.load_node(parent_id, buffer_pool)?;
        let child_id = parent.children[child_index];
        let mut child = self.load_node(child_id, buffer_pool)?;
        
        // Create new right child
        let mut new_child = Node {
            keys: child.keys.split_off(T),
            children: if child.is_leaf {
                Vec::new()
            } else {
                child.children.split_off(T)
            },
            is_leaf: child.is_leaf,
        };
        
        let up_key = child.keys.pop().expect("Should exist during split");
        
        // Allocate page for new child
        let new_child_id = self.allocate_node(new_child, buffer_pool)?;
        
        // Update parent
        parent.keys.insert(child_index, up_key);
        parent.children.insert(child_index + 1, new_child_id);
        
        // Save all modified nodes
        self.save_node(parent_id, &parent, buffer_pool)?;
        self.save_node(child_id, &child, buffer_pool)?;
        
        Ok(())
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
        data[offset..offset + 4].copy_from_slice(&child_count.to_le_bytes());
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







/* 
Visual Walkthrough of insert_non_full

Scenario: Insert key PageId(25) into this B-tree (T=3, so max keys = 5):

Initial B-tree:
                Root (PageId=1)
             [10, 30, 50, 70]
            /    |    |    |   \
        Child0 Child1 Child2 Child3 Child4
      PageId=2 PageId=3 PageId=4 PageId=5 PageId=6
       [5,8]    [15,20]  [35,40]  [55,60]  [75,80]

Step 1: Load the root node

let mut node = self.load_node(page_id, buffer_pool)?; // Load Root (PageId=1)
// node.keys = [10, 30, 50, 70]
// node.children = [2, 3, 4, 5, 6]
// node.is_leaf = false

Step 2: Find insertion position

let mut i = node.keys.len(); // i = 4
if node.is_leaf { // false, so skip this branch
    // ...
} else {
    // Find where key=25 should go
    while i > 0 && key < node.keys[i - 1] {
        i -= 1;
    }
}

Visual trace of the while loop:

key = 25
i = 4: 25 < 70? Yes → i = 3
i = 3: 25 < 50? Yes → i = 2  
i = 2: 25 < 30? Yes → i = 1
i = 1: 25 < 10? No → stop
Final: i = 1

So key=25 should go in child[1] (between keys 10 and 30).

Step 3: Check if child needs splitting

let child_id = node.children[i]; // child_id = PageId(3)
let child_node = self.load_node(child_id, buffer_pool)?;
// child_node.keys = [15, 20]  (only 2 keys, not full)

if child_node.keys.len() == 2 * T - 1 { // 2 != 5, so false
    // Skip splitting
}

Child1 (PageId=3): [15, 20]  ← Only 2 keys (max is 5), so no split needed

Step 4: Recursively insert into child

let updated_node = self.load_node(page_id, buffer_pool)?; // Reload root
self.insert_non_full(updated_node.children[i], key, buffer_pool)?;
// Calls insert_non_full(PageId(3), 25, buffer_pool)

Step 5: Recursive call on child (PageId=3)

// Now we're in the child node
let mut node = self.load_node(PageId(3), buffer_pool)?;
// node.keys = [15, 20]
// node.is_leaf = true

let mut i = node.keys.len(); // i = 2
if node.is_leaf { // true!
    while i > 0 && key < node.keys[i - 1] {
        i -= 1;
    }
    // i=2: 25 < 20? No → stop
    // Final: i = 2
    
    node.keys.insert(i, key); // Insert 25 at position 2
    // node.keys = [15, 20, 25]
    
    self.save_node(page_id, &node, buffer_pool)?; // Save the modified child
}


Final Result:
Root (PageId=1)
[10, 30, 50, 70]
/    |    |    |   \
[5,8] [15,20,25] [35,40] [55,60] [75,80]
       ↑ Added 25 here
*/