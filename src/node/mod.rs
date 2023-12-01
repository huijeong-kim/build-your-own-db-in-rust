use crate::node_layout::*;
use crate::pager::{PAGE_SIZE, Pager};
use crate::table::Table;
use crate::node::internal_node::{initialize_internal_node, internal_node_child, internal_node_key, internal_node_num_keys, internal_node_right_child};
use crate::node::leaf_node::{leaf_node_key, leaf_node_num_cells};

pub mod leaf_node;
pub mod internal_node;

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

unsafe fn copy_node(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, PAGE_SIZE as usize);
}
