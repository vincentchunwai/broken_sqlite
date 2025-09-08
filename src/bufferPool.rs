use std::collections::{HashMap, VecDeque};
use std::io;

mod page;
use pager::{Pager, Page, PageId, PAGE_SIZE};

pub struct BufferPool {
    page: Pager,
    cache: HashMap<PageId, Page>,
    lru: VecDeque<PageId>,
    max_pages: usize,
}

impl BufferPool {
    pub fn new(pager: Pager, capacity: usize) -> Self {
        Self {
            pager,
            cache: HashMap::new(),
            lru: VecDeque::new(),
            capacity
        }
    }

    // Fetch a page from the buffer pool (load from disk if not cached)
    pub fn get_page(&mut self, id: PageId) -> io::Result<&mut Page> {
        if self.cache.contains_key(&id) {
            self.promote(id);
        } else {
            if self.cache.len() >= self.capacity {
                self.evict_one()?;
            }
            let page = self.pager.read_page(id)?;
            self.cache.insert(id, page);
            self.lru.push_back(id);
        }
        Ok(self.cache.get_mut(&id).unwrap())
    }

    // Mark a page dirty after modification
    pub fn mark_dirty(&mut self, id: PageId) {
        if let Some(page) = self.cache.get_mut(&id) {
            page.dirty = true;
            self.promote(id);
        }
    }

    // Flush all dirty pages to disk
    pub fn flush(&mut self) -> io::Result<()> {
        for page in self.cache.values_mut() {
            if page.dirty {
                self.pager.write_page(page)?;
                page.dirty = false;
            }
        }

        Ok(())
    }

    // Evict one page using LRU
    fn evict_one(&mut self) -> io::Result<()> {
        if let Some(victim_id) = self.lru.pop_front() {
            if let Some(page) = self.cache.remove(&victim_id) {
                if page.dirty {
                    self.pager.write_page(&page)?;
                }
            }
        }
        Ok(())
    }

    // Promote a page to most recently used in LRU
    fn promote(&mut self, id: PageId) {
        if let Some(pos) = self.lru.iter().position(|&x| x == id) {
            self.lru.remove(pos);
        }
        self.lru.push_back(id);
    }
}
