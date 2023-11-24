use crate::node_layout::{leaf_node_num_cells, leaf_node_value};
use crate::table::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_num: usize,
    cell_num: u8,
    end_of_table: bool,
}

pub fn table_start(table: &mut Table) -> Cursor {
    let root_page_num = table.root_page_num();
    let pager = table.pager();
    let root_node = pager.page(root_page_num);
    let num_cells = unsafe { *leaf_node_num_cells(root_node) };

    Cursor {
        table,
        page_num: root_page_num,
        cell_num: 0,
        end_of_table: (num_cells == 0),
    }
}

pub fn table_end(table: &mut Table) -> Cursor {
    let root_page_num = table.root_page_num();
    let root_node = table.pager().page(root_page_num);
    let num_cells = unsafe { *leaf_node_num_cells(root_node) };

    Cursor {
        table,
        page_num: root_page_num,
        cell_num: num_cells,
        end_of_table: true,
    }
}

impl Cursor<'_> {
    pub fn advance(&mut self) {
        let node = self.page();
        self.cell_num += 1;

        unsafe {
            if self.cell_num >= *(leaf_node_num_cells(node)) {
                self.end_of_table = true;
            }
        }
    }

    pub unsafe fn value(&mut self) -> *mut u8 {
        let page = self.page();
        leaf_node_value(page, self.cell_num)
    }

    pub fn end_of_table(&self) -> bool {
        self.end_of_table
    }

    pub fn page(&mut self) -> *mut u8 {
        self.table.pager().page(self.page_num as usize)
    }

    pub fn cell_num(&self) -> u8 {
        self.cell_num
    }
}
