use crate::cursor::Cursor;
use crate::data::{serialize_row, Row};
use crate::node_layout::{
    leaf_node_cell, leaf_node_key, leaf_node_num_cells, leaf_node_value, LEAF_NODE_CELL_SIZE,
    LEAF_NODE_MAX_CELLS, NODE_TYPE_OFFSET,
};
use libc::EXIT_FAILURE;
use std::process::exit;

#[derive(PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}
impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => panic!("Invalid value for NodeType"),
        }
    }
}
impl From<NodeType> for u8 {
    fn from(value: NodeType) -> Self {
        match value {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        }
    }
}

pub unsafe fn leaf_node_insert(cursor: &mut Cursor, key: u8, value: &Row) {
    let node = cursor.page();

    let num_cells = *leaf_node_num_cells(node);
    if num_cells >= LEAF_NODE_MAX_CELLS as u8 {
        // Node full
        println!("Need to implement splitting a leaf node");
        exit(EXIT_FAILURE);
    }

    if cursor.cell_num() < num_cells {
        for i in (cursor.cell_num() + 1..=num_cells).rev() {
            std::ptr::copy_nonoverlapping(
                leaf_node_cell(node, i - 1),
                leaf_node_cell(node, i),
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor.cell_num())) = key;
    serialize_row(value, leaf_node_value(node, cursor.cell_num()));
}

pub unsafe fn get_node_type(node: *mut u8) -> NodeType {
    let value = *node.add(NODE_TYPE_OFFSET);
    value.into()
}

pub unsafe fn set_node_type(node: *mut u8, node_type: NodeType) {
    let value = node_type.into();
    std::ptr::write(node.add(NODE_TYPE_OFFSET), value);
}

pub unsafe fn print_leaf_node(node: *mut u8) {
    let num_cells = *leaf_node_num_cells(node);
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = *leaf_node_key(node, i);
        println!("   - {} : {}", i, key);
    }
}
