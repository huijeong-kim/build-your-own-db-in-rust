use crate::node::{
    get_node_type, internal_node_child, internal_node_key, internal_node_num_keys, NodeType,
};
use crate::node::{leaf_node_key, leaf_node_num_cells, leaf_node_value};
use crate::pager::Pager;
use crate::table::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_num: u32,
    cell_num: u32,
    end_of_table: bool,
}

#[allow(dead_code)]
pub fn table_start(table: &mut Table) -> Cursor {
    let root_page_num = table.root_page_num();
    let pager = table.pager();
    let root_node = pager.page(root_page_num);
    let num_cells = unsafe { std::ptr::read(leaf_node_num_cells(root_node) as *const u32) };

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
    let num_cells = unsafe { std::ptr::read(leaf_node_num_cells(root_node) as *const u32) };

    Cursor {
        table,
        page_num: root_page_num,
        cell_num: num_cells,
        end_of_table: true,
    }
}
pub unsafe fn table_find(table: &mut Table, key: u32) -> Cursor {
    let root_page_num = table.root_page_num();
    let root_node = table.pager().page(root_page_num);

    return if get_node_type(root_node) == NodeType::Leaf {
        leaf_node_find(table, root_page_num, key)
    } else {
        internal_node_find(table, root_page_num, key)
    };
}

pub unsafe fn leaf_node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
    let node = table.pager().page(page_num);
    let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);

    // Binary search
    let mut min_index = 0u32;
    let mut one_past_max_index = num_cells as u32;

    while one_past_max_index != min_index {
        let index = (min_index + one_past_max_index) / 2;
        let key_at_index = std::ptr::read(leaf_node_key(node, index) as *const u32);

        if key == key_at_index {
            return Cursor {
                table,
                page_num,
                cell_num: index,
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
        cell_num: min_index,
        end_of_table: false,
    }
}

unsafe fn internal_node_find(table: &mut Table, page_number: u32, key: u32) -> Cursor {
    let node = table.pager().page(page_number);
    let num_keys = std::ptr::read(internal_node_num_keys(node) as *const u32);

    let mut min_index = 0u32;
    let mut max_index = num_keys;

    while min_index != max_index {
        let index = (min_index + max_index) / 2;
        let key_to_right = std::ptr::read(internal_node_key(node, index) as *const u32);
        if key_to_right >= key {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    let child_num = std::ptr::read(internal_node_child(node, min_index) as *const u32);
    let child = table.pager().page(child_num);
    match get_node_type(child) {
        NodeType::Internal => leaf_node_find(table, child_num, key),
        NodeType::Leaf => internal_node_find(table, child_num, key),
    }
}

impl Cursor<'_> {
    pub fn advance(&mut self) {
        let node = self.page();
        self.cell_num += 1;

        unsafe {
            if self.cell_num >= std::ptr::read(leaf_node_num_cells(node) as *const u32) {
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

    pub fn table(&mut self) -> &mut Table {
        &mut self.table
    }
    pub fn pager(&mut self) -> &mut Pager {
        self.table.pager()
    }

    pub fn cell_num(&self) -> u32 {
        self.cell_num
    }
}
