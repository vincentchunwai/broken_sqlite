mod page;

use page::PageId;

// Each node except root must have at least t-1 keys and at most 2t-1 keys
const T: usize = 2;

#[derive(Debug)]
pub struct BTree {
    root: Node
}

#[derive(Debug, Default)]
pub struct Node {
    keys: Vec<u32>,
    children: Vec<PageId>,
    is_leaf: bool
}

impl Node {
    fn empty_leaf() -> std::io::Result<Self> {
        Ok(Node {
            keys: Vec::new(),
            children: Vec::new(),
            is_leaf: true
        })
    }

    fn split_child(&mut self, i: usize) {
        let mut y = self.children.remove(i);
        let mut z = Node {
            keys: y.keys.split_off(T),
            children: if y.is_leaf {
                Vec::new()
            } else {
                y.children.split_off(T) // takes the last (len - T) keys 
            },
            is_leaf: y.is_leaf,
        };
        
        let up_key = y.keys.pop().expect("Should exist during split"); // takes middle key

        self.keys.insert(i, up_key);
        self.children.insert(i, y);
        self.children.insert(i + 1, z);
    }
}







