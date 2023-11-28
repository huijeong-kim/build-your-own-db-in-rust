use crate::node::{get_node_type, NodeType};
use crate::node::{leaf_node_key, leaf_node_num_cells, leaf_node_value};
use crate::table::Table;
use libc::EXIT_FAILURE;
use std::process::exit;

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_num: usize,
    cell_num: u8,
    end_of_table: bool,
}

#[allow(dead_code)]
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
pub unsafe fn table_find(table: &mut Table, key: u8) -> Cursor {
    let root_page_num = table.root_page_num();
    let root_node = table.pager().page(root_page_num);

    if get_node_type(root_node) == NodeType::Leaf {
        return leaf_node_find(table, root_page_num, key);
    } else {
        println!("Need to implement searching an internal node");
        exit(EXIT_FAILURE);
    }
}

pub unsafe fn leaf_node_find(table: &mut Table, page_num: usize, key: u8) -> Cursor {
    let node = table.pager().page(page_num);
    let num_cells = *leaf_node_num_cells(node);

    // Binary search
    let mut min_index = 0u32;
    let mut one_past_max_index = num_cells as u32;

    while one_past_max_index != min_index {
        let index = (min_index + one_past_max_index) / 2;
        let key_at_index = *leaf_node_key(node, index as u8);

        if key == key_at_index {
            return Cursor {
                table,
                page_num,
                cell_num: index as u8,
                end_of_table: false,
            };
        }

        if key < key_at_index {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    Cursor {
        table,
        page_num,
        cell_num: min_index as u8,
        end_of_table: false,
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
        self.table.pager().page(self.page_num)
    }

    pub fn cell_num(&self) -> u8 {
        self.cell_num
    }
}
