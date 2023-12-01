use libc::{exit, EXIT_FAILURE};
use crate::cursor::internal_node_find_child;
use crate::node::{create_new_root, get_node_max_key, is_node_root, node_parent, NodeType, set_node_root, set_node_type};
use crate::node_layout::{INTERNAL_NODE_CELL_SIZE, INTERNAL_NODE_CHILD_SIZE, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_MAX_CELLS, INTERNAL_NODE_NUM_KEYS_OFFSET, INTERNAL_NODE_RIGHT_CHILD_OFFSET};
use crate::table::{INVALID_PAGE_NUM, Table};

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

pub unsafe fn internal_node_insert(table: &mut Table, parent_page_num: u32, child_page_num: u32) {
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


pub unsafe fn update_internal_node_key(node: *mut u8, old_key: u32, new_key: u32) {
    let old_child_index = internal_node_find_child(node, old_key);
    std::ptr::write(
        internal_node_key(node, old_child_index) as *mut u32,
        new_key,
    );
}

pub unsafe fn internal_node_num_keys(node: *mut u8) -> *mut u8 {
    node.add(INTERNAL_NODE_NUM_KEYS_OFFSET)
}

pub unsafe fn internal_node_right_child(node: *mut u8) -> *mut u8 {
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
    internal_node_cell(node, key_num).add(INTERNAL_NODE_CHILD_SIZE)
}

pub unsafe fn initialize_internal_node(node: *mut u8) {
    set_node_type(node, NodeType::Internal);
    set_node_root(node, false);
    std::ptr::write(internal_node_num_keys(node) as *mut u32, 0);
    std::ptr::write(
        internal_node_right_child(node) as *mut u32,
        INVALID_PAGE_NUM,
    );
}
unsafe fn copy_internal_cell(src: (*mut u8, u32), dest: (*mut u8, u32)) {
    let src_cell = internal_node_cell(src.0, src.1);
    let dest_cell = internal_node_cell(dest.0, dest.1);
    std::ptr::copy_nonoverlapping(src_cell, dest_cell, INTERNAL_NODE_CELL_SIZE);
}