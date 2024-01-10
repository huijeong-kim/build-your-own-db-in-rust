use crate::cursor::Cursor;
use crate::node::internal_node::{internal_node_insert, update_internal_node_key, InternalNode};
use crate::node::{create_new_root, NodeTrait, NodeType};
use crate::node_layout::{
    LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_KEY_SIZE, LEAF_NODE_LEFT_SPLIT_COUNT,
    LEAF_NODE_MAX_CELLS, LEAF_NODE_NEXT_LEAF_OFFSET, LEAF_NODE_NUM_CELLS_OFFSET,
    LEAF_NODE_RIGHT_SPLIT_COUNT,
};
use crate::pager::{Pager, TABLE_MAX_PAGES};
use crate::row::{serialize_row, Row};
use crate::statement::ExecuteResult;

pub fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) -> ExecuteResult {
    let node = cursor.leaf_node();

    unsafe {
        let num_cells = node.get_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
            // Node full
            return leaf_node_split_and_insert(cursor, key, value);
        }

        if cursor.cell_num() < num_cells {
            for i in (cursor.cell_num() + 1..=num_cells).rev() {
                copy_leaf_cell(node.cell(i - 1), node.cell(i));
            }
        }

        //node.set_next_leaf(num_cells);
        node.set_num_cells(num_cells + 1);
        node.set_key(cursor.cell_num(), key);

        serialize_row(value, node.value(cursor.cell_num()));
    }
    ExecuteResult::Success
}

unsafe fn leaf_node_split_and_insert(cursor: &mut Cursor, key: u32, value: &Row) -> ExecuteResult {
    let old_node = cursor.leaf_node();
    let old_max = old_node.get_node_max_key(cursor.pager());

    let new_page_num = cursor.pager().get_unused_page_num();
    if new_page_num >= TABLE_MAX_PAGES {
        return ExecuteResult::TableFull;
    }

    let new_node = cursor.pager().page(new_page_num);
    let new_node = LeafNode::new(new_node);
    new_node.initialize();
    new_node.set_parent(old_node.get_parent());
    new_node.set_next_leaf(old_node.get_next_leaf());

    old_node.set_next_leaf(new_page_num);

    for i in (0..=LEAF_NODE_MAX_CELLS as u32).rev() {
        let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT as u32 {
            &new_node
        } else {
            &old_node
        };

        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT as u32;
        // let index_within_node = if i > LEAF_NODE_LEFT_SPLIT_COUNT {
        //     i % LEAF_NODE_RIGHT_SPLIT_COUNT
        // } else {
        //     i % LEAF_NODE_LEFT_SPLIT_COUNT
        // };
        if i == cursor.cell_num() {
            let dest = destination_node.value(index_within_node);
            serialize_row(value, dest);
            destination_node.set_key(index_within_node, key);
        } else if i > cursor.cell_num() {
            let src = old_node.cell(i - 1);
            let dest = destination_node.cell(index_within_node);
            copy_leaf_cell(src, dest);
        } else {
            let src = old_node.cell(i);
            let dest = destination_node.cell(index_within_node);
            copy_leaf_cell(src, dest);
        }
    }

    old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT as u32);
    new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT as u32);

    if old_node.is_root() {
        create_new_root(cursor.table(), new_page_num);
    } else {
        let parent_page_num = old_node.get_parent();
        let new_max = old_node.get_node_max_key(cursor.pager());
        let parent = cursor.pager().page(parent_page_num);
        let parent = InternalNode::new(parent);
        update_internal_node_key(&parent, old_max, new_max);
        internal_node_insert(cursor.table(), parent_page_num, new_page_num);
    }

    ExecuteResult::Success
}

pub unsafe fn get_leaf_node_num_cells(node: *mut u8) -> u32 {
    let data = LeafNode::new(node);
    data.get_num_cells()
}

pub unsafe fn get_leaf_node_key(node: *mut u8, cell_num: u32) -> u32 {
    let node = LeafNode::new(node);
    node.get_key(cell_num)
}

unsafe fn copy_leaf_cell(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, LEAF_NODE_CELL_SIZE);
}

pub struct LeafNode {
    data: *mut u8,
}
impl NodeTrait for LeafNode {
    fn data(&self) -> *mut u8 {
        self.data
    }
    // unsafe fn get_node_max_key(&self, _pager: &mut Pager) -> u32 {
    //     let num_cells = self.get_num_cells();
    //     self.get_key(num_cells - 1)
    // }
}

impl LeafNode {
    pub fn new(data: *mut u8) -> Self {
        Self { data }
    }
    pub unsafe fn initialize(&self) {
        self.set_num_cells(0);
        self.set_next_leaf(0);
        self.set_node_type(NodeType::Leaf);
        self.set_root(false);
    }

    pub unsafe fn value(&self, cell_num: u32) -> *mut u8 {
        self.cell(cell_num).add(LEAF_NODE_KEY_SIZE)
    }

    pub unsafe fn next_leaf(&self) -> *mut u8 {
        self.data.add(LEAF_NODE_NEXT_LEAF_OFFSET)
    }
    pub unsafe fn get_next_leaf(&self) -> u32 {
        std::ptr::read(self.data.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *const u32)
    }
    pub unsafe fn set_next_leaf(&self, next: u32) {
        std::ptr::write(self.data.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32, next);
    }

    pub unsafe fn cell(&self, cell_num: u32) -> *mut u8 {
        self.data
            .add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
    }
    pub fn get_num_cells(&self) -> u32 {
        unsafe { std::ptr::read(self.data.add(LEAF_NODE_NUM_CELLS_OFFSET) as *const u32) }
    }
    pub unsafe fn set_num_cells(&self, num_cells: u32) {
        std::ptr::write(
            self.data.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32,
            num_cells,
        )
    }
    pub fn get_key(&self, cell_num: u32) -> u32 {
        unsafe { std::ptr::read(self.cell(cell_num) as *const u32) }
    }
    pub unsafe fn set_key(&self, cell_num: u32, key: u32) {
        std::ptr::write(self.cell(cell_num) as *mut u32, key);
    }

    #[allow(dead_code)]
    pub unsafe fn print_leaf_node(&self) {
        let num_cells = self.get_num_cells();
        println!("leaf (size {})", num_cells);
        for i in 0..num_cells {
            let key = self.get_key(i);
            println!("   - {} : {}", i, key);
        }
    }
}
