use crate::cursor::Cursor;
use crate::node::{create_new_root, get_node_max_key, is_node_root, node_parent, NodeType, set_node_root, set_node_type};
use crate::node::internal_node::{internal_node_insert, update_internal_node_key};
use crate::node_layout::{LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_KEY_SIZE, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS, LEAF_NODE_NEXT_LEAF_OFFSET, LEAF_NODE_NUM_CELLS_OFFSET, LEAF_NODE_RIGHT_SPLIT_COUNT};
use crate::row::{Row, serialize_row};

pub unsafe fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let node = cursor.page();

    let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);
    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if cursor.cell_num() < num_cells {
        for i in (cursor.cell_num() + 1..=num_cells).rev() {
            copy_leaf_cell((node, i - 1), (node, i));
        }
    }

    let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);
    std::ptr::write(leaf_node_num_cells(node) as *mut u32, num_cells + 1);

    std::ptr::write(leaf_node_key(node, cursor.cell_num()) as *mut u32, key);
    serialize_row(value, leaf_node_value(node, cursor.cell_num()));
}

unsafe fn leaf_node_split_and_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let old_node = cursor.page();
    let old_max = get_node_max_key(cursor.pager(), old_node);
    let new_page_num = cursor.pager().get_unused_page_num();
    let new_node = cursor.pager().page(new_page_num);
    initialize_leaf_node(new_node);

    let old_node_parent = std::ptr::read(node_parent(old_node) as *const u32);
    std::ptr::write(node_parent(new_node) as *mut u32, old_node_parent);

    let old_node_next = std::ptr::read(leaf_node_next_leaf(old_node) as *const u32);
    std::ptr::write(leaf_node_next_leaf(new_node) as *mut u32, old_node_next);
    std::ptr::write(leaf_node_next_leaf(old_node) as *mut u32, new_page_num);

    for i in (0..=LEAF_NODE_MAX_CELLS as u32).rev() {
        let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT as u32 {
            new_node
        } else {
            old_node
        };

        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT as u32;
        // let index_within_node = if i > LEAF_NODE_LEFT_SPLIT_COUNT {
        //     i % LEAF_NODE_RIGHT_SPLIT_COUNT
        // } else {
        //     i % LEAF_NODE_LEFT_SPLIT_COUNT
        // };
        if i == cursor.cell_num() {
            let destination = leaf_node_value(destination_node, index_within_node);
            serialize_row(value, destination);
            let destination_key = leaf_node_key(destination_node, index_within_node);
            std::ptr::write(destination_key as *mut u32, key);
        } else if i > cursor.cell_num() {
            copy_leaf_cell((old_node, i - 1), (destination_node, index_within_node));
        } else {
            copy_leaf_cell((old_node, i), (destination_node, index_within_node));
        }
    }

    std::ptr::write(
        leaf_node_num_cells(old_node) as *mut u32,
        LEAF_NODE_LEFT_SPLIT_COUNT as u32,
    );
    std::ptr::write(
        leaf_node_num_cells(new_node) as *mut u32,
        LEAF_NODE_RIGHT_SPLIT_COUNT as u32,
    );

    if is_node_root(old_node) {
        create_new_root(cursor.table(), new_page_num.try_into().unwrap());
    } else {
        let parent_page_num = std::ptr::read(node_parent(old_node) as *const u32);
        let new_max = get_node_max_key(cursor.pager(), old_node);
        let parent = cursor.pager().page(parent_page_num);
        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor.table(), parent_page_num, new_page_num);
    }
}

#[allow(dead_code)]
pub unsafe fn print_leaf_node(node: *mut u8) {
    let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = std::ptr::read(leaf_node_key(node, i) as *const u32);
        println!("   - {} : {}", i, key);
    }
}

pub unsafe fn leaf_node_num_cells(node: *mut u8) -> *mut u8 {
    node.add(LEAF_NODE_NUM_CELLS_OFFSET)
}

pub unsafe fn leaf_node_cell(node: *mut u8, cell_num: u32) -> *mut u8 {
    node.add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
}

pub unsafe fn leaf_node_key(node: *mut u8, cell_num: u32) -> *mut u8 {
    leaf_node_cell(node, cell_num)
}

pub unsafe fn leaf_node_value(node: *mut u8, cell_num: u32) -> *mut u8 {
    leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE)
}

pub unsafe fn leaf_node_next_leaf(node: *mut u8) -> *mut u8 {
    node.add(LEAF_NODE_NEXT_LEAF_OFFSET)
}

pub unsafe fn initialize_leaf_node(node: *mut u8) {
    std::ptr::write(leaf_node_num_cells(node) as *mut u32, 0);
    std::ptr::write(leaf_node_next_leaf(node) as *mut u32, 0);
    set_node_type(node, NodeType::Leaf);
    set_node_root(node, false);
}
unsafe fn copy_leaf_cell(src: (*mut u8, u32), dest: (*mut u8, u32)) {
    let src_cell = leaf_node_cell(src.0, src.1);
    let dest_cell = leaf_node_cell(dest.0, dest.1);
    std::ptr::copy_nonoverlapping(src_cell, dest_cell, LEAF_NODE_CELL_SIZE);
}