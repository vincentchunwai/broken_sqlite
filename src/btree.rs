mod page;

use page::PageId;

// Each node except root must have at least t-1 keys and at most 2t-1 keys
const T: usize = 16; // Max 31 keys, 32 children to avoid stack overflow

#[derive(Debug)]
pub struct BTree {
    root: Node
}

#[derive(Debug, Default)]
pub struct Node {
    keys: Vec<PageId>,
    children: Vec<Node>,
    is_leaf: bool
}

impl BTree {
    fn new() -> Self {
        BTree {
            root: Node {
                keys: Vec::new(),
                children: Vec::new(),
                is_leaf: true,
            },
        }
    }

    fn insert(&mut self, key: PageId) {
        if self.root.keys.len() == 2 * T - 1 {
            let mut new_root = Node {
                keys: Vec::new(),
                children: Vec::new(),
                is_leaf: false,
            };
            new_root.children.push(std::mem::replace(&mut self.root, Node::empty_leaf()));
            new_root.split_child(0);
            new_root.insert_non_full(key);
            self.root = new_root;
        } else {
            self.root.insert_non_full(key);
        }
    }

    fn contains(&self, key: PageId) -> bool {
        self.root.search(key).is_some()
    }

    fn print_inorder(&self) {
        self.root.print_inorder();
        println!();
    }
}
impl Node {
    fn empty_leaf() -> Self {
        Node {
            keys: Vec::new(),
            children: Vec::new(),
            is_leaf: true
        }
    }

    fn search(&self, key: PageId) -> Option<&PageId> {
        let mut i = 0;
        while i < self.keys.len() && key > &self.keys[i] {
            i++;
        }

        if i < self.keys.len() && key == &self.keys[i] {
            return Some(&self.keys[i]);
        }

        if self.is_leaf {
            None
        } else {
            self.children[i].search(key)
        }
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

    fn insert_non_full(&mut self, key: PageId){
        let mut i = self.keys.len();
        if self.is_leaf {
            while i > 0 && key < self.keys[i - 1] {
                i -= 1;
            }
            self.keys.insert(i, key);
        } else {
            while i > 0 && key < self.keys[i - 1] {
                i -= 1;
            }
            if self.children[i].keys.len() == 2 * T - 1 {
                self.split_child(i);
                if key > self.keys[i] {
                    i += 1;
                }
            }
            self.children[i].insert_non_full(key);
        }
    }

    fn print_inorder(&self) {
        if self.is_leaf {
            for k in &self.keys {
                print!("{} ", k.clone());
            }
        } else {
            for i in 0..self.keys.len() {
                self.children[i].print_inorder();
                print!("{} ", self.keys[i].clone());
            }
            self.children[self.keys.len()].print_inorder();
        }
    }
}







