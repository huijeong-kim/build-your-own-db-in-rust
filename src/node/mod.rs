use crate::node_layout::*;
use crate::pager::{PAGE_SIZE, Pager};
use crate::table::Table;
use crate::node::internal_node::{get_internal_node_key, get_internal_node_num_keys, get_internal_node_right_child, initialize_internal_node, internal_node_child, set_internal_node_key, set_internal_node_num_keys, set_internal_node_right_child};
use crate::node::leaf_node::{get_leaf_node_key, get_leaf_node_num_cells};

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
        let left_child_num_keys = get_internal_node_num_keys(left_child);
        for i in 0..left_child_num_keys {
            let left_left_child = std::ptr::read(internal_node_child(left_child, i) as *const u32);
            let child = table.pager().page(left_left_child);
            std::ptr::write(node_parent(child) as *mut u32, left_child_page_num);
        }
        let left_right_child = get_internal_node_right_child(left_child);
        let child = table.pager().page(left_right_child);
        std::ptr::write(node_parent(child) as *mut u32, left_child_page_num);
    }

    // reset root as internal node
    initialize_internal_node(root);
    set_node_root(root, true);
    set_internal_node_num_keys(root, 1);
    std::ptr::write(
        internal_node_child(root, 0) as *mut u32,
        left_child_page_num,
    );
    let left_child_max_key = get_node_max_key(table.pager(), left_child);
    set_internal_node_key(root, 0, left_child_max_key);
    set_internal_node_right_child(root, right_child_page_number);

    std::ptr::write(node_parent(left_child) as *mut u32, table.root_page_num());
    std::ptr::write(node_parent(right_child) as *mut u32, table.root_page_num());
}

pub unsafe fn get_node_type(node: *mut u8) -> NodeType {
    let node = Node::new(node);
    node.get_node_type()
}

fn indent(level: usize) {
    let indent = "  ".repeat(level);
    print!("{}", indent);
}

pub unsafe fn print_tree(pager: &mut Pager, page_num: u32, indentation_level: usize) {
    let node = pager.page(page_num);

    match get_node_type(node) {
        NodeType::Internal => {
            let num_keys = get_internal_node_num_keys(node);
            indent(indentation_level);
            println!("- internal (size {})", num_keys);

            if num_keys > 0 {
                for i in 0..num_keys {
                    let child = std::ptr::read(internal_node_child(node, i) as *const u32);
                    print_tree(pager, child, indentation_level + 1);

                    indent(indentation_level + 1);
                    println!(
                        "- key {}",
                        get_internal_node_key(node, i)
                    );
                }
                let child = get_internal_node_right_child(node);
                print_tree(pager, child, indentation_level + 1);
            }
        }
        NodeType::Leaf => {
            let num_keys = get_leaf_node_num_cells(node);
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);
            for i in 0..num_keys {
                indent(indentation_level + 1);
                println!("- {}", get_leaf_node_key(node, i));
            }
        }
    }
}

unsafe fn node_parent(node: *mut u8) -> *mut u8 {
    let node = Node::new(node);
    node.parent()
}

unsafe fn get_node_max_key(pager: &mut Pager, node: *mut u8) -> u32 {
    let node = Node::new(node);
    node.get_node_max_key(pager)
}

struct Node {
    data: *mut u8,
}
impl Node {
    pub fn new(data: *mut u8) -> Self {
        Self { data }
    }
    pub unsafe fn is_root(&self) -> bool {
        let value = std::ptr::read(self.data.add(IS_ROOT_OFFSET) as *const u8);
        return if value == 0 { false } else if value == 1 { true } else { panic!("invalid value"); }
    }
    pub unsafe fn set_root(&self, is_root: bool) {
        std::ptr::write(
            self.data.add(IS_ROOT_OFFSET),
            if is_root == true { 1u8 } else { 0u8 },
        );
    }
    pub unsafe fn parent(self) -> *mut u8 {
        self.data.add(PARENT_POINTER_OFFSET)
    }

    pub unsafe fn get_parent(&self) -> u32 {
        std::ptr::read(self.data.add(PARENT_POINTER_OFFSET) as *const u32)
    }
    pub unsafe fn set_parent(&self, parent: u32)  {
        std::ptr::write(self.data.add(PARENT_POINTER_OFFSET) as *mut u32, parent);
    }

    pub unsafe fn get_node_type(&self) -> NodeType {
        let value = *self.data.add(NODE_TYPE_OFFSET);
        value.into()
    }
    pub unsafe fn set_node_type(&self,  node_type: NodeType) {
        let value = node_type.into();
        std::ptr::write(self.data.add(NODE_TYPE_OFFSET), value);
    }

    unsafe fn get_node_max_key(&self, pager: &mut Pager) -> u32 {
        match self.get_node_type() {
            NodeType::Internal => {
                let right_child_page_num = get_internal_node_right_child(self.data);
                let right_child = pager.page(right_child_page_num);
                get_node_max_key(pager, right_child)
            }
            NodeType::Leaf => {
                let num_cells = get_leaf_node_num_cells(self.data);
                get_leaf_node_key(self.data, num_cells - 1)
            }
        }
    }

}
unsafe fn is_node_root(node: *mut u8) -> bool {
    let node = Node::new(node);
    node.is_root()
}

pub unsafe fn set_node_root(node: *mut u8, is_root: bool) {
    let node = Node::new(node);
    node.set_root(is_root);
}

unsafe fn copy_node(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, PAGE_SIZE as usize);
}
