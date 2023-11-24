use crate::data::ROW_SIZE;
use crate::node::{set_node_type, NodeType};
use crate::table::PAGE_SIZE;

// Node header
const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
pub const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf node header
const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf node body
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: u8 = (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE) as u8;

pub unsafe fn leaf_node_num_cells(node: *mut u8) -> *mut u8 {
    node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut _
}

pub unsafe fn leaf_node_cell(node: *mut u8, cell_num: u8) -> *mut u8 {
    node.add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
}

pub unsafe fn leaf_node_key(node: *mut u8, cell_num: u8) -> *mut u8 {
    leaf_node_cell(node, cell_num)
}

pub unsafe fn leaf_node_value(node: *mut u8, cell_num: u8) -> *mut u8 {
    leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE)
}

pub unsafe fn initialize_leaf_node(node: *mut u8) {
    std::ptr::write(leaf_node_num_cells(node), 0);
    set_node_type(node, NodeType::Leaf);
}

pub fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}
