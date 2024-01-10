use crate::node::internal_node::{
    get_internal_node_right_child, set_internal_node_right_child, InternalNode,
};
use crate::node::leaf_node::{get_leaf_node_key, get_leaf_node_num_cells, LeafNode};
use crate::node_layout::*;
use crate::pager::{Pager, PAGE_SIZE};
use crate::table::Table;

pub mod internal_node;
pub mod leaf_node;

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
    let right_child = InternalNode::new(right_child);

    let left_child_page_num = table.pager().get_unused_page_num();
    let left_child = table.pager().page(left_child_page_num);
    let left_child = InternalNode::new(left_child);

    if get_node_type(root) == NodeType::Internal {
        right_child.initialize();
        left_child.initialize();
    }

    // Copy root data to new node(left_child)
    copy_node(root, left_child.node());

    // left_child is internal node
    left_child.set_root(false);

    if left_child.get_node_type() == NodeType::Internal {
        let left_child_num_keys = left_child.get_num_keys();
        for i in 0..left_child_num_keys {
            let left_left_child = left_child.get_child(i);
            let child = table.pager().page(left_left_child);
            let child_node = LeafNode::new(child);
            child_node.set_parent(left_child_page_num);
        }
        let left_right_child = left_child.get_right_child();
        let child = table.pager().page(left_right_child);
        let child_node = LeafNode::new(child);
        child_node.set_parent(left_child_page_num);
    }

    // reset root as internal node
    let root = InternalNode::new(root);
    root.initialize();
    root.set_root(true);
    root.set_num_keys(1);
    std::ptr::write(root.child(0) as *mut u32, left_child_page_num);
    let left_child_max_key = get_node_max_key(table.pager(), left_child.node());
    //let left_child_max_key = left_child.get_node_max_key(table.pager());
    root.set_key(0, left_child_max_key);
    set_internal_node_right_child(root.node(), right_child_page_number);

    left_child.set_parent(table.root_page_num());
    right_child.set_parent(table.root_page_num());
}

pub unsafe fn get_node_type(data: *mut u8) -> NodeType {
    let value = *data.add(NODE_TYPE_OFFSET);
    value.into()
}

fn indent(level: usize) {
    let indent = "  ".repeat(level);
    print!("{}", indent);
}

pub fn print_tree(pager: &mut Pager, page_num: u32, indentation_level: usize) {
    let node = pager.page(page_num);

    unsafe {
        match get_node_type(node) {
            NodeType::Internal => {
                let node = InternalNode::new(node);
                let num_keys = node.get_num_keys();
                indent(indentation_level);
                println!("- internal (size {})", num_keys);

                if num_keys > 0 {
                    for i in 0..num_keys {
                        let child = node.get_child(i);
                        print_tree(pager, child, indentation_level + 1);

                        indent(indentation_level + 1);
                        println!("- key {}", node.get_key(i));
                    }
                    let child = node.get_right_child();
                    print_tree(pager, child, indentation_level + 1);
                }
            }
            NodeType::Leaf => {
                let node = LeafNode::new(node);
                let num_keys = node.get_num_cells();
                indent(indentation_level);
                println!("- leaf (size {})", num_keys);
                for i in 0..num_keys {
                    indent(indentation_level + 1);
                    println!("- {}", node.get_key(i));
                }
            }
        }
    }
}

unsafe fn node_parent(node: *mut u8) -> *mut u8 {
    let node = LeafNode::new(node);
    node.parent()
}

unsafe fn get_node_max_key(pager: &mut Pager, node: *mut u8) -> u32 {
    let node = LeafNode::new(node);
    node.get_node_max_key(pager)
}

struct Node {
    data: *mut u8,
}
impl Node {
    pub fn new(data: *mut u8) -> Self {
        Self { data }
    }
}
impl NodeTrait for Node {
    fn data(&self) -> *mut u8 {
        self.data
    }
    unsafe fn get_node_max_key(&self, pager: &mut Pager) -> u32 {
        panic!();
    }
}

pub trait NodeTrait {
    fn data(&self) -> *mut u8;

    unsafe fn get_node_max_key(&self, pager: &mut Pager) -> u32 {
        match self.get_node_type() {
            NodeType::Internal => {
                let right_child_page_num = get_internal_node_right_child(self.data());
                let right_child = pager.page(right_child_page_num);
                get_node_max_key(pager, right_child)
            }
            NodeType::Leaf => {
                let num_cells = get_leaf_node_num_cells(self.data());
                get_leaf_node_key(self.data(), num_cells - 1)
            }
        }
    }

    unsafe fn is_root(&self) -> bool {
        let value = std::ptr::read(self.data().add(IS_ROOT_OFFSET) as *const u8);
        return if value == 0 {
            false
        } else if value == 1 {
            true
        } else {
            panic!("invalid value");
        };
    }
    unsafe fn set_root(&self, is_root: bool) {
        std::ptr::write(
            self.data().add(IS_ROOT_OFFSET),
            if is_root == true { 1u8 } else { 0u8 },
        );
    }
    unsafe fn parent(&self) -> *mut u8 {
        self.data().add(PARENT_POINTER_OFFSET)
    }

    unsafe fn get_parent(&self) -> u32 {
        std::ptr::read(self.data().add(PARENT_POINTER_OFFSET) as *const u32)
    }
    unsafe fn set_parent(&self, parent: u32) {
        std::ptr::write(self.data().add(PARENT_POINTER_OFFSET) as *mut u32, parent);
    }

    unsafe fn get_node_type(&self) -> NodeType {
        let value = *self.data().add(NODE_TYPE_OFFSET);
        value.into()
    }
    unsafe fn set_node_type(&self, node_type: NodeType) {
        let value = node_type.into();
        std::ptr::write(self.data().add(NODE_TYPE_OFFSET), value);
    }
}
unsafe fn is_node_root(node: *mut u8) -> bool {
    let node = LeafNode::new(node);
    node.is_root()
}

unsafe fn copy_node(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, PAGE_SIZE as usize);
}
