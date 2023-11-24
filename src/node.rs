use crate::cursor::Cursor;
use crate::data::{serialize_row, Row};
use crate::node_layout::{
    leaf_node_cell, leaf_node_key, leaf_node_num_cells, leaf_node_value, LEAF_NODE_CELL_SIZE,
    LEAF_NODE_MAX_CELLS,
};
use libc::EXIT_FAILURE;
use std::process::exit;

pub unsafe fn leaf_node_insert(cursor: &mut Cursor, key: u8, value: &Row) {
    let node = cursor.page();

    let num_cells = *leaf_node_num_cells(node);
    if num_cells >= LEAF_NODE_MAX_CELLS as u8 {
        // Node full
        println!("Need to implement splitting a leaf node");
        exit(EXIT_FAILURE);
    }

    if cursor.cell_num() < num_cells {
        for i in num_cells..cursor.cell_num() {
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

pub unsafe fn print_leaf_node(node: *mut u8) {
    let num_cells = *leaf_node_num_cells(node);
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = *leaf_node_key(node, i);
        println!("   - {} : {}", i, key);
    }
}