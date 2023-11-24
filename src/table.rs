use crate::data::{deserialize_row, serialize_row, Row, ROW_SIZE};
use crate::pager::Pager;
use crate::statement::ExecuteResult;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    num_rows: usize,
    pager: Option<Pager>,
}
impl Table {
    pub fn new() -> Self {
        Self {
            num_rows: 0,
            pager: None,
        }
    }
    pub fn db_open(&mut self, filename: &String) {
        let pager = Pager::open(filename);
        self.num_rows = pager.file_size() as usize / ROW_SIZE;
        self.pager = Some(pager);
    }

    pub fn db_close(&mut self) {
        match &mut self.pager {
            Some(p) => p.close(self.num_rows),
            None => {}
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), ExecuteResult> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(ExecuteResult::TableFull);
        }

        let (page_num, byte_offset) = self.row_slot(self.num_rows);

        unsafe {
            let page_ptr = self.pager.as_mut().unwrap().page(page_num).add(byte_offset) as *mut _;
            serialize_row(&row, page_ptr);
        }

        self.num_rows += 1;

        Ok(())
    }

    pub fn select(&mut self) -> Result<(), ExecuteResult> {
        let mut row = Row::new();
        for i in 0..self.num_rows {
            let (page_num, byte_offset) = self.row_slot(i);

            unsafe {
                let page_ptr =
                    self.pager.as_mut().unwrap().page(page_num).add(byte_offset) as *const _;
                deserialize_row(page_ptr, &mut row);
            }

            println!("{}", row);
        }

        Ok(())
    }

    pub fn row_slot(&mut self, row_num: usize) -> (usize, usize) {
        let page_num = row_num / ROWS_PER_PAGE;

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        (page_num, byte_offset)
    }
}
