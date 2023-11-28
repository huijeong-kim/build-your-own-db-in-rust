use crate::cursor::Cursor;
use crate::node_layout::{
    INTERNAL_NODE_CELL_SIZE, INTERNAL_NODE_CHILD_SIZE, INTERNAL_NODE_HEADER_SIZE,
    INTERNAL_NODE_NUM_KEYS_OFFSET, INTERNAL_NODE_RIGHT_CHILD_OFFSET, IS_ROOT_OFFSET,
    LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_KEY_SIZE, LEAF_NODE_LEFT_SPLIT_COUNT,
    LEAF_NODE_MAX_CELLS, LEAF_NODE_NUM_CELLS_OFFSET, LEAF_NODE_RIGHT_SPLIT_COUNT, NODE_TYPE_OFFSET,
};
use crate::pager::Pager;
use crate::row::{serialize_row, Row};
use crate::table::{Table, PAGE_SIZE};
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
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if cursor.cell_num() < num_cells {
        for i in (cursor.cell_num() + 1..=num_cells).rev() {
            copy_cell((node, i - 1), (node, i));
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor.cell_num())) = key;
    serialize_row(value, leaf_node_value(node, cursor.cell_num()));
}

unsafe fn leaf_node_split_and_insert(cursor: &mut Cursor, _key: u8, value: &Row) {
    let old_node = cursor.page();
    let new_page_num = cursor.pager().get_unused_page_num();
    let new_node = cursor.pager().page(new_page_num);
    initialize_leaf_node(new_node);

    for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
        let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
            new_node
        } else {
            old_node
        };

        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        // let index_within_node = if i > LEAF_NODE_LEFT_SPLIT_COUNT {
        //     i % LEAF_NODE_RIGHT_SPLIT_COUNT
        // } else {
        //     i % LEAF_NODE_LEFT_SPLIT_COUNT
        // };
        if i == cursor.cell_num() {
            let destination = leaf_node_cell(destination_node, index_within_node);
            serialize_row(value, destination);
        } else if i > cursor.cell_num() {
            copy_cell((old_node, i - 1), (destination_node, index_within_node));
        } else {
            copy_cell((old_node, i), (destination_node, index_within_node));
        }
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if is_node_root(old_node) {
        create_new_root(cursor.table(), new_page_num.try_into().unwrap());
    } else {
        println!("Need to implement updating parent after split");
        exit(EXIT_FAILURE);
    }
}

unsafe fn create_new_root(table: &mut Table, right_child_page_number: usize) {
    let root_page_num = table.root_page_num();
    let root = table.pager().page(root_page_num);

    let left_child_page_num = table.pager().get_unused_page_num();
    let left_child = table.pager().page(left_child_page_num);

    // Copy root data to new node(left_child)
    copy_node(root, left_child);

    // left_child is internal node
    set_node_root(left_child, false);

    // reset root as internal node
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num as u8;
    let left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_number as u8;
}

pub unsafe fn get_node_type(node: *mut u8) -> NodeType {
    let value = *node.add(NODE_TYPE_OFFSET);
    value.into()
}

pub unsafe fn set_node_type(node: *mut u8, node_type: NodeType) {
    let value = node_type.into();
    std::ptr::write(node.add(NODE_TYPE_OFFSET), value);
}

#[allow(dead_code)]
pub unsafe fn print_leaf_node(node: *mut u8) {
    let num_cells = *leaf_node_num_cells(node);
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = *leaf_node_key(node, i);
        println!("   - {} : {}", i, key);
    }
}

fn indent(level: usize) {
    let indent = "  ".repeat(level);
    print!("{}", indent);
}

pub unsafe fn print_tree(pager: &mut Pager, page_num: usize, indentation_level: usize) {
    let node = pager.page(page_num);

    match get_node_type(node) {
        NodeType::Internal => {
            let num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            println!("- internal (size {})", num_keys);

            for i in 0..num_keys {
                let child = *internal_node_child(node, i);
                print_tree(pager, child as usize, indentation_level + 1);

                indent(indentation_level + 1);
                println!("- key {}", *internal_node_key(node, i));
            }
            let child = *internal_node_right_child(node);
            print_tree(pager, child as usize, indentation_level + 1);
        }
        NodeType::Leaf => {
            let num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);
            for i in 0..num_keys {
                indent(indentation_level + 1);
                println!("- {}", *leaf_node_key(node, i));
            }
        }
    }
}

pub unsafe fn leaf_node_num_cells(node: *mut u8) -> *mut u8 {
    node.add(LEAF_NODE_NUM_CELLS_OFFSET)
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
    set_node_root(node, false);
}

pub unsafe fn internal_node_num_keys(node: *mut u8) -> *mut u8 {
    node.add(INTERNAL_NODE_NUM_KEYS_OFFSET)
}

unsafe fn internal_node_right_child(node: *mut u8) -> *mut u8 {
    node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET)
}

unsafe fn internal_node_cell(node: *mut u8, cell_num: u8) -> *mut u8 {
    node.add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
}

pub unsafe fn internal_node_child(node: *mut u8, child_num: u8) -> *mut u8 {
    let num_keys = *internal_node_num_keys(node);

    if child_num > num_keys {
        println!(
            "Tried to access child num {} > num_keys {}",
            child_num, num_keys
        );
        exit(EXIT_FAILURE);
    } else if child_num == num_keys {
        internal_node_right_child(node)
    } else {
        internal_node_cell(node, child_num)
    }
}

pub unsafe fn internal_node_key(node: *mut u8, key_num: u8) -> *mut u8 {
    internal_node_cell(node, key_num).add(INTERNAL_NODE_CHILD_SIZE)
}

unsafe fn initialize_internal_node(node: *mut u8) {
    set_node_type(node, NodeType::Internal);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

unsafe fn get_node_max_key(node: *mut u8) -> u8 {
    match get_node_type(node) {
        NodeType::Internal => *internal_node_key(node, *internal_node_num_keys(node) - 1),
        NodeType::Leaf => *leaf_node_key(node, *leaf_node_num_cells(node) - 1),
    }
}

unsafe fn is_node_root(node: *mut u8) -> bool {
    let value = *node.add(IS_ROOT_OFFSET);
    return if value == 0 {
        false
    } else if value == 1 {
        true
    } else {
        panic!("invalid value");
    };
}

pub unsafe fn set_node_root(node: *mut u8, is_root: bool) {
    *node.add(IS_ROOT_OFFSET) = if is_root == true { 1u8 } else { 0u8 };
}

unsafe fn copy_cell(src: (*mut u8, u8), dest: (*mut u8, u8)) {
    let src_cell = leaf_node_cell(src.0, src.1);
    let dest_cell = leaf_node_cell(dest.0, dest.1);
    std::ptr::copy_nonoverlapping(src_cell, dest_cell, LEAF_NODE_CELL_SIZE);
}

unsafe fn copy_node(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, PAGE_SIZE);
}
