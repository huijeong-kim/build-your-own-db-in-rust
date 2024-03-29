use crate::node::internal_node::{internal_node_find_child, InternalNode};
use crate::node::leaf_node::LeafNode;
use crate::node::{get_node_type, NodeType};
use crate::pager::Pager;
use crate::table::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_num: u32,
    cell_num: u32,
    end_of_table: bool,
}

pub fn table_start(table: &mut Table) -> Cursor {
    let mut cursor = table_find(table, 0);
    let node = cursor.page();
    let node = LeafNode::new(node);
    let num_cells = node.get_num_cells();
    cursor.end_of_table = num_cells == 0;

    cursor
}

pub fn table_end(table: &mut Table) -> Cursor {
    let root_page_num = table.root_page_num();
    let root_node = table.pager().page(root_page_num);
    let root_node = LeafNode::new(root_node);
    let num_cells = root_node.get_num_cells();

    Cursor {
        table,
        page_num: root_page_num,
        cell_num: num_cells,
        end_of_table: true,
    }
}
pub fn table_find(table: &mut Table, key: u32) -> Cursor {
    let root_page_num = table.root_page_num();
    let root_node = table.pager().page(root_page_num);

    unsafe {
        return if get_node_type(root_node) == NodeType::Leaf {
            leaf_node_find(table, root_page_num, key)
        } else {
            internal_node_find(table, root_page_num, key)
        };
    }
}

pub unsafe fn leaf_node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
    let node = table.pager().page(page_num);
    let node = LeafNode::new(node);
    let num_cells = node.get_num_cells();

    // Binary search
    let mut min_index = 0u32;
    let mut one_past_max_index = num_cells;

    while one_past_max_index != min_index {
        let index = (min_index + one_past_max_index) / 2;
        let key_at_index = node.get_key(index);

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
    let node = InternalNode::new(node);
    let child_index = internal_node_find_child(&node, key);
    let child_num = node.get_child(child_index);
    let child = table.pager().page(child_num);
    match get_node_type(child) {
        NodeType::Internal => internal_node_find(table, child_num, key),
        NodeType::Leaf => leaf_node_find(table, child_num, key),
    }
}

impl Cursor<'_> {
    pub fn advance(&mut self) {
        let node = self.page();
        let node = LeafNode::new(node);
        self.cell_num += 1;

        unsafe {
            if self.cell_num >= node.get_num_cells() {
                let next_page_num = node.get_next_leaf();
                if next_page_num == 0 {
                    self.end_of_table = true;
                } else {
                    self.page_num = next_page_num;
                    self.cell_num = 0;
                }
            }
        }
    }

    pub fn value(&mut self) -> *mut u8 {
        let page = self.page();
        let node = LeafNode::new(page);
        unsafe { node.value(self.cell_num) }
    }

    pub fn end_of_table(&self) -> bool {
        self.end_of_table
    }

    pub fn page(&mut self) -> *mut u8 {
        self.table.pager().page(self.page_num)
    }

    pub fn leaf_node(&mut self) -> LeafNode {
        LeafNode::new(self.page())
    }
    pub fn internal_node(&mut self) -> InternalNode {
        InternalNode::new(self.page())
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
