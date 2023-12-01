use crate::cursor::{internal_node_find_child, Cursor};
use crate::node_layout::*;
use crate::pager::Pager;
use crate::row::{serialize_row, Row};
use crate::table::{Table, INVALID_PAGE_NUM, PAGE_SIZE};
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

unsafe fn update_internal_node_key(node: *mut u8, old_key: u32, new_key: u32) {
    let old_child_index = internal_node_find_child(node, old_key);
    std::ptr::write(
        internal_node_key(node, old_child_index) as *mut u32,
        new_key,
    );
}

unsafe fn internal_node_insert(table: &mut Table, parent_page_num: u32, child_page_num: u32) {
    let parent = table.pager().page(parent_page_num);
    let child = table.pager().page(child_page_num);
    let child_max_key = get_node_max_key(table.pager(), child);
    let index = internal_node_find_child(parent, child_max_key);

    let original_num_keys = std::ptr::read(internal_node_num_keys(parent) as *const u32);
    if original_num_keys >= INTERNAL_NODE_MAX_CELLS as u32 {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    let right_child_page_num = std::ptr::read(internal_node_right_child(parent) as *mut u32);
    if right_child_page_num == INVALID_PAGE_NUM {
        // empty internal node. add to right child
        std::ptr::write(
            internal_node_right_child(parent) as *mut u32,
            child_page_num,
        );
        return;
    }

    let right_child = table.pager().page(right_child_page_num);
    std::ptr::write(
        internal_node_num_keys(parent) as *mut u32,
        original_num_keys + 1,
    );

    if child_max_key > get_node_max_key(table.pager(), right_child) {
        // Replace right child
        std::ptr::write(
            internal_node_child(parent, original_num_keys) as *mut u32,
            right_child_page_num,
        );
        std::ptr::write(
            internal_node_key(parent, original_num_keys) as *mut u32,
            get_node_max_key(table.pager(), right_child),
        );
        std::ptr::write(
            internal_node_right_child(parent) as *mut u32,
            child_page_num,
        );
    } else {
        // Add to new cell
        for i in (index + 1..=original_num_keys).rev() {
            copy_internal_cell((parent, i - 1), (parent, i));
        }

        std::ptr::write(
            internal_node_child(parent, index) as *mut u32,
            child_page_num,
        );
        std::ptr::write(internal_node_key(parent, index) as *mut u32, child_max_key);
    }
}

unsafe fn internal_node_split_and_insert(
    table: &mut Table,
    parent_page_num: u32,
    child_page_num: u32,
) {
    let mut old_page_num = parent_page_num;
    let mut old_node = table.pager().page(old_page_num);
    let old_max = get_node_max_key(table.pager(), old_node);

    let child = table.pager().page(child_page_num);
    let child_max = get_node_max_key(table.pager(), child);

    let new_page_num = table.pager().get_unused_page_num();

    let splitting_root = is_node_root(old_node);
    let (parent, new_node) = if splitting_root {
        create_new_root(table, new_page_num);
        let root_page_num = table.root_page_num();
        let parent = table.pager().page(root_page_num);

        old_page_num = std::ptr::read(internal_node_child(parent, 0) as *const u32);
        old_node = table.pager().page(old_page_num);

        (parent, None)
    } else {
        let old_node_parent = std::ptr::read(node_parent(old_node) as *const u32);
        let parent = table.pager().page(old_node_parent);
        let new_node = table.pager().page(new_page_num);
        initialize_internal_node(new_node);

        (parent, Some(new_node))
    };

    let old_num_keys = internal_node_num_keys(old_node);

    let mut cur_page_num = std::ptr::read(internal_node_right_child(old_node) as *const u32);
    let mut cur = table.pager().page(cur_page_num);

    // First put right child into new node and set right child of old node to invalid page number
    internal_node_insert(table, new_page_num, cur_page_num);
    std::ptr::write(node_parent(cur) as *mut u32, new_page_num);
    std::ptr::write(
        internal_node_right_child(old_node) as *mut u32,
        INVALID_PAGE_NUM,
    );

    // For each key until you get to the middle key, move the key and the child to the new node
    for i in (INTERNAL_NODE_MAX_CELLS / 2 + 1..INTERNAL_NODE_MAX_CELLS).rev() {
        cur_page_num = std::ptr::read(internal_node_child(old_node, i as u32) as *const u32);
        cur = table.pager().page(cur_page_num);

        internal_node_insert(table, new_page_num, cur_page_num);
        std::ptr::write(node_parent(cur) as *mut u32, new_page_num);

        let old_num_keys_value = std::ptr::read(old_num_keys as *const u32);
        std::ptr::write(old_num_keys as *mut u32, old_num_keys_value - 1);
    }

    // Set child before middle key, which is now the highest key, to be node's right child and decrement number of keys
    let old_num_keys_value = std::ptr::read(old_num_keys as *const u32);
    let new_right_child =
        std::ptr::read(internal_node_child(old_node, old_num_keys_value - 1) as *const u32);
    std::ptr::write(
        internal_node_right_child(old_node) as *mut u32,
        new_right_child,
    );
    std::ptr::write(old_num_keys as *mut u32, old_num_keys_value - 1);

    // Determine which of the two nodes after the split should contain the child to be inserted and insert the child
    let max_after_split = get_node_max_key(table.pager(), old_node);
    let destination_page_num = if child_max < max_after_split {
        old_page_num
    } else {
        new_page_num
    };

    internal_node_insert(table, destination_page_num, child_page_num);
    std::ptr::write(node_parent(child) as *mut u32, destination_page_num);

    update_internal_node_key(parent, old_max, get_node_max_key(table.pager(), old_node));

    if !splitting_root {
        let old_node_parent = std::ptr::read(node_parent(old_node) as *const u32);
        internal_node_insert(table, old_node_parent, new_page_num);
        std::ptr::write(node_parent(new_node.unwrap()) as *mut u32, old_node_parent);
    }
}

unsafe fn create_new_root(table: &mut Table, right_child_page_number: u32) {
    let root_page_num = table.root_page_num();
    let root = table.pager().page(root_page_num);

    let right_child = table.pager().page(right_child_page_number);

    let left_child_page_num = table.pager().get_unused_page_num();
    let left_child = table.pager().page(left_child_page_num);

    if get_node_type(root) == NodeType::Internal {
        initialize_internal_node(right_child);
        initialize_internal_node(left_child);
    }

    // Copy root data to new node(left_child)
    copy_node(root, left_child);

    // left_child is internal node
    set_node_root(left_child, false);

    if get_node_type(left_child) == NodeType::Internal {
        let left_child_num_keys = std::ptr::read(internal_node_num_keys(left_child) as *const u32);
        for i in 0..left_child_num_keys {
            let left_left_child = std::ptr::read(internal_node_child(left_child, i) as *const u32);
            let child = table.pager().page(left_left_child);
            std::ptr::write(node_parent(child) as *mut u32, left_child_page_num);
        }
        let left_right_child = std::ptr::read(internal_node_right_child(left_child) as *const u32);
        let child = table.pager().page(left_right_child);
        std::ptr::write(node_parent(child) as *mut u32, left_child_page_num);
    }

    // reset root as internal node
    initialize_internal_node(root);
    set_node_root(root, true);
    std::ptr::write(internal_node_num_keys(root) as *mut u32, 1);
    std::ptr::write(
        internal_node_child(root, 0) as *mut u32,
        left_child_page_num,
    );
    let left_child_max_key = get_node_max_key(table.pager(), left_child);
    std::ptr::write(internal_node_key(root, 0) as *mut u32, left_child_max_key);
    std::ptr::write(
        internal_node_right_child(root) as *mut u32,
        right_child_page_number,
    );

    std::ptr::write(node_parent(left_child) as *mut u32, table.root_page_num());
    std::ptr::write(node_parent(right_child) as *mut u32, table.root_page_num());
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
    let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = std::ptr::read(leaf_node_key(node, i) as *const u32);
        println!("   - {} : {}", i, key);
    }
}

fn indent(level: usize) {
    let indent = "  ".repeat(level);
    print!("{}", indent);
}

pub unsafe fn print_tree(pager: &mut Pager, page_num: u32, indentation_level: usize) {
    let node = pager.page(page_num);

    match get_node_type(node) {
        NodeType::Internal => {
            let num_keys = std::ptr::read(internal_node_num_keys(node) as *const u32);
            indent(indentation_level);
            println!("- internal (size {})", num_keys);

            if num_keys > 0 {
                for i in 0..num_keys {
                    let child = std::ptr::read(internal_node_child(node, i) as *const u32);
                    print_tree(pager, child, indentation_level + 1);

                    indent(indentation_level + 1);
                    println!(
                        "- key {}",
                        std::ptr::read(internal_node_key(node, i) as *const u32)
                    );
                }
                let child = std::ptr::read(internal_node_right_child(node) as *const u32);
                print_tree(pager, child, indentation_level + 1);
            }
        }
        NodeType::Leaf => {
            let num_keys = std::ptr::read(leaf_node_num_cells(node) as *const u32);
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);
            for i in 0..num_keys {
                indent(indentation_level + 1);
                println!("- {}", std::ptr::read(leaf_node_key(node, i) as *const u32));
            }
        }
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

pub unsafe fn internal_node_num_keys(node: *mut u8) -> *mut u8 {
    node.add(INTERNAL_NODE_NUM_KEYS_OFFSET)
}

unsafe fn internal_node_right_child(node: *mut u8) -> *mut u8 {
    node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET)
}

unsafe fn internal_node_cell(node: *mut u8, cell_num: u32) -> *mut u8 {
    node.add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
}

pub unsafe fn internal_node_child(node: *mut u8, child_num: u32) -> *mut u8 {
    let num_keys = std::ptr::read(internal_node_num_keys(node) as *const u32);

    if child_num > num_keys {
        println!(
            "Tried to access child num {} > num_keys {}",
            child_num, num_keys
        );
        exit(EXIT_FAILURE);
    } else if child_num == num_keys {
        let right_child = std::ptr::read(internal_node_right_child(node) as *const u32);
        if right_child == INVALID_PAGE_NUM {
            println!("Tried to access child of node, but was invalid page");
            exit(EXIT_FAILURE);
        }

        internal_node_right_child(node)
    } else {
        // child_num < num_keys
        let child = internal_node_cell(node, child_num);
        if std::ptr::read(child as *const u32) == INVALID_PAGE_NUM {
            println!(
                "Tried to access child {} of node, but was invalid page",
                child_num
            );
            exit(EXIT_FAILURE);
        }

        child
    }
}

pub unsafe fn internal_node_key(node: *mut u8, key_num: u32) -> *mut u8 {
    internal_node_cell(node, key_num).add(INTERNAL_NODE_CHILD_SIZE as usize)
}

unsafe fn initialize_internal_node(node: *mut u8) {
    set_node_type(node, NodeType::Internal);
    set_node_root(node, false);
    std::ptr::write(internal_node_num_keys(node) as *mut u32, 0);
    std::ptr::write(
        internal_node_right_child(node) as *mut u32,
        INVALID_PAGE_NUM,
    );
}

unsafe fn node_parent(node: *mut u8) -> *mut u8 {
    node.add(PARENT_POINTER_OFFSET)
}

unsafe fn get_node_max_key(pager: &mut Pager, node: *mut u8) -> u32 {
    match get_node_type(node) {
        NodeType::Internal => {
            let right_child_page_num =
                std::ptr::read(internal_node_right_child(node) as *const u32);
            let right_child = pager.page(right_child_page_num);
            get_node_max_key(pager, right_child)
        }
        NodeType::Leaf => {
            let num_cells = std::ptr::read(leaf_node_num_cells(node) as *const u32);
            std::ptr::read(leaf_node_key(node, num_cells - 1) as *const u32)
        }
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
    std::ptr::write(
        node.add(IS_ROOT_OFFSET),
        if is_root == true { 1u8 } else { 0u8 },
    );
}

unsafe fn copy_leaf_cell(src: (*mut u8, u32), dest: (*mut u8, u32)) {
    let src_cell = leaf_node_cell(src.0, src.1);
    let dest_cell = leaf_node_cell(dest.0, dest.1);
    std::ptr::copy_nonoverlapping(src_cell, dest_cell, LEAF_NODE_CELL_SIZE);
}

unsafe fn copy_internal_cell(src: (*mut u8, u32), dest: (*mut u8, u32)) {
    let src_cell = internal_node_cell(src.0, src.1);
    let dest_cell = internal_node_cell(dest.0, dest.1);
    std::ptr::copy_nonoverlapping(src_cell, dest_cell, INTERNAL_NODE_CELL_SIZE);
}

unsafe fn copy_node(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, PAGE_SIZE as usize);
}
