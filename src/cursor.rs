use crate::data::ROW_SIZE;
use crate::table::{Table, ROWS_PER_PAGE};

pub struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

pub fn table_start(table: &mut Table) -> Cursor {
    let num_rows = table.num_rows();
    Cursor {
        table,
        row_num: 0,
        end_of_table: (num_rows == 0),
    }
}

pub fn table_end(table: &mut Table) -> Cursor {
    let num_rows = table.num_rows();
    Cursor {
        table,
        row_num: num_rows,
        end_of_table: true,
    }
}

impl Cursor<'_> {
    pub(crate) fn advance(&mut self) {
        self.row_num += 1;

        if self.row_num >= self.table.num_rows() {
            self.end_of_table = true;
        }
    }

    pub unsafe fn value(&mut self) -> *mut u8 {
        let page_num = self.row_num / ROWS_PER_PAGE;
        let row_offset = self.row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        self.table.pager().page(page_num).add(byte_offset) as *mut _
    }

    pub fn end_of_table(&self) -> bool {
        self.end_of_table
    }
}
