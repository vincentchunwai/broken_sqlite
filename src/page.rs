use std::io::{Write, Seek, SeekFrom};
use std::cmp::Ordering;
use std::fs::*;
use std::io::Read;

pub const PAGE_SIZE: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PageId(pub u64);

pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub dirty: bool,
}

pub struct Pager {
    file: std::fs::File,
}

impl Pager {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Pager { file })
    }

    pub fn read_page(&mut self, id: PageId) -> std::io::Result<Page> {
        let offset = id.0 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;
        Ok(Page {
            id,
            data: buf,
            dirty: false,
        })
    }

    pub fn write_page(&mut self, page: &Page) -> std::io::Result<()> {
        let offset = page.id.0 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> std::io::Result<Page> {
        let len = self.file.metadata()?.len();
        let new_id = PageId(len / PAGE_SIZE as u64);

        let page = Page {
            id: new_id,
            data: [0u8; PAGE_SIZE],
            dirty: true
        };

        self.write_page(&page)?;

        Ok(page)
    }
}
