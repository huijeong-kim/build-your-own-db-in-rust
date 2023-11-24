use crate::data::{deserialize_row, serialize_row, Row, ROW_SIZE};
use crate::statement::ExecuteResult;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    num_rows: usize,
    pages: [[u8; PAGE_SIZE]; TABLE_MAX_PAGES],
}
impl Table {
    pub fn new() -> Self {
        Table {
            num_rows: 0,
            pages: [[0; PAGE_SIZE]; TABLE_MAX_PAGES],
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), ExecuteResult> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(ExecuteResult::TableFull);
        }

        let (page_num, byte_offset) = self.row_slot(self.num_rows);

        unsafe {
            let page_ptr = self.pages[page_num].as_ref().as_ptr().add(byte_offset) as *mut _;
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
                let page_ptr = self.pages[page_num].as_ref().as_ptr().add(byte_offset) as *const _;
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
