use crate::cursor::{table_find, table_start};
use crate::data::{deserialize_row, Row};
use crate::node::leaf_node_insert;
use crate::node_layout::{
    initialize_leaf_node, leaf_node_key, leaf_node_num_cells, LEAF_NODE_MAX_CELLS,
};
use crate::pager::Pager;
use crate::statement::ExecuteResult;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Table {
    root_page_num: usize,
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

    pub fn insert(&mut self, row: Row) -> Result<(), ExecuteResult> {
        let node = self.pager.as_mut().unwrap().page(self.root_page_num);

        unsafe {
            let num_cells = *leaf_node_num_cells(node);
            if num_cells >= LEAF_NODE_MAX_CELLS {
                return Err(ExecuteResult::TableFull);
            }

            let key_to_insert = row.id as u8;
            let mut cursor = table_find(self, key_to_insert);

            if cursor.cell_num() < num_cells {
                let key_at_index = *leaf_node_key(node, cursor.cell_num());
                if key_at_index == key_to_insert {
                    return Err(ExecuteResult::DuplicateKey);
                }
            }

            leaf_node_insert(&mut cursor, row.id as u8, &row);
        }

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

    pub fn root_page_num(&self) -> usize {
        self.root_page_num
    }

    pub fn pager(&mut self) -> &mut Pager {
        self.pager.as_mut().unwrap()
    }
}
