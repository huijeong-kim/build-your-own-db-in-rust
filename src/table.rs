use crate::cursor::{table_find, table_start};
use crate::node::{
    initialize_leaf_node, leaf_node_insert, leaf_node_key, leaf_node_num_cells, print_tree,
    set_node_root,
};
use crate::pager::Pager;
use crate::row::{deserialize_row, Row};
use crate::statement::ExecuteResult;

pub const PAGE_SIZE: u32 = 4096;
pub const TABLE_MAX_PAGES: u32 = 100;

pub struct Table {
    root_page_num: u32,
    pager: Option<Pager>,
}
impl Table {
    pub fn new() -> Self {
        Self {
            root_page_num: 0,
            pager: None,
        }
    }
    pub fn db_open(&mut self, filename: &String) {
        let mut pager = Pager::open(filename);
        self.root_page_num = 0;

        if pager.num_pages() == 0 {
            // New database file. Initialize page 0 as leaf node.
            let root_node = pager.page(0);
            unsafe {
                initialize_leaf_node(root_node);
                set_node_root(root_node, true);
            }
        }
        self.pager = Some(pager);
    }

    pub fn db_close(&mut self) {
        match &mut self.pager {
            Some(p) => p.close(),
            None => {}
        }
    }

    pub fn insert(&mut self, row: Row) -> ExecuteResult {
        let root_page_num = self.root_page_num;
        let node = self.pager().page(root_page_num);

        unsafe {
            let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);
            let key_to_insert = row.id;
            let mut cursor = table_find(self, key_to_insert);

            if cursor.cell_num() < num_cells {
                let key_at_index =
                    std::ptr::read(leaf_node_key(node, cursor.cell_num()) as *const u32);
                if key_at_index == key_to_insert {
                    return ExecuteResult::DuplicateKey;
                }
            }

            leaf_node_insert(&mut cursor, row.id, &row);
        }

        ExecuteResult::Success
    }

    pub fn select(&mut self) -> ExecuteResult {
        let mut row = Row::new();

        let mut cursor = table_start(self);
        while !cursor.end_of_table() {
            unsafe {
                deserialize_row(cursor.value(), &mut row);
            }

            println!("{}", row);
            cursor.advance();
        }

        ExecuteResult::Success
    }

    pub fn print(&mut self) {
        unsafe {
            print_tree(self.pager(), 0, 0);
        }
    }

    pub fn root_page_num(&self) -> u32 {
        self.root_page_num
    }

    pub fn pager(&mut self) -> &mut Pager {
        self.pager.as_mut().unwrap()
    }
}
