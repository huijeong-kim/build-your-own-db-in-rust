use crate::cursor::{table_end, table_start};
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

        let mut cursor = table_end(self);
        unsafe {
            serialize_row(&row, cursor.value());
        }

        self.num_rows += 1;

        Ok(())
    }

    pub fn select(&mut self) -> Result<(), ExecuteResult> {
        let mut row = Row::new();

        let mut cursor = table_start(self);
        while !cursor.end_of_table() {
            unsafe {
                deserialize_row(cursor.value(), &mut row);
            }

            println!("{}", row);
            cursor.advance();
        }

        Ok(())
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn pager(&mut self) -> &mut Pager {
        self.pager.as_mut().unwrap()
    }
}
