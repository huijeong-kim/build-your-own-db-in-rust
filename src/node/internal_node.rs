use crate::cursor::internal_node_find_child;
use crate::node::{create_new_root, get_node_max_key, is_node_root, node_parent, Node, NodeType};
use crate::node_layout::{
    INTERNAL_NODE_CELL_SIZE, INTERNAL_NODE_CHILD_SIZE, INTERNAL_NODE_HEADER_SIZE,
    INTERNAL_NODE_MAX_CELLS, INTERNAL_NODE_NUM_KEYS_OFFSET, INTERNAL_NODE_RIGHT_CHILD_OFFSET,
    IS_ROOT_OFFSET, NODE_TYPE_OFFSET, PARENT_POINTER_OFFSET,
};
use crate::pager::Pager;
use crate::table::{Table, INVALID_PAGE_NUM};
use libc::{exit, EXIT_FAILURE};

unsafe fn internal_node_split_and_insert(
    table: &mut Table,
    parent_page_num: u32,
    child_page_num: u32,
) {
    let old_page_num = parent_page_num;
    let old_node = table.pager().page(old_page_num);

    if is_node_root(old_node) {
        internal_node_split_and_insert_root(table, parent_page_num, child_page_num);
    } else {
        internal_node_split_and_insert_non_root(table, parent_page_num, child_page_num);
    }
}

unsafe fn internal_node_split_and_insert_root(
    table: &mut Table,
    parent_page_num: u32,
    child_page_num: u32,
) {
    let mut old_page_num = parent_page_num;
    let old = table.pager().page(old_page_num);
    let old_node = InternalNode::new(old);
    let old_max = old_node.get_node_max_key(table.pager());

    let child = table.pager().page(child_page_num);
    let child = InternalNode::new(child);
    let child_max = child.get_node_max_key(table.pager());

    let new_page_num = table.pager().get_unused_page_num();

    create_new_root(table, new_page_num);
    let root_page_num = table.root_page_num();
    let parent = table.pager().page(root_page_num);
    let parent = InternalNode::new(parent);

    old_page_num = parent.get_child(0);
    let old_node = table.pager().page(old_page_num);
    let old_node = InternalNode::new(old_node);

    let mut cur_page_num = old_node.get_right_child();
    let cur = table.pager().page(cur_page_num);
    let cur = Node::new(cur);

    // First put right child into new node and set right child of old node to invalid page number
    internal_node_insert(table, new_page_num, cur_page_num);

    cur.set_parent(new_page_num);
    old_node.set_right_child(INVALID_PAGE_NUM);

    // For each key until you get to the middle key, move the key and the child to the new node
    for i in (INTERNAL_NODE_MAX_CELLS / 2 + 1..INTERNAL_NODE_MAX_CELLS).rev() {
        cur_page_num = old_node.get_child(i as u32);
        let cur = table.pager().page(cur_page_num);
        let cur = Node::new(cur);

        internal_node_insert(table, new_page_num, cur_page_num);
        cur.set_parent(new_page_num);

        let old_num_keys_value = old_node.get_num_keys();
        old_node.set_num_keys(old_num_keys_value - 1);
    }

    // Set child before middle key, which is now the highest key, to be node's right child and decrement number of keys
    let old_num_keys_value = old_node.get_num_keys();
    let new_right_child = old_node.get_child(old_num_keys_value - 1);

    old_node.set_right_child(new_right_child);
    old_node.set_num_keys(old_num_keys_value - 1);

    // Determine which of the two nodes after the split should contain the child to be inserted and insert the child
    let max_after_split = old_node.get_node_max_key(table.pager());
    let destination_page_num = if child_max < max_after_split {
        old_page_num
    } else {
        new_page_num
    };

    internal_node_insert(table, destination_page_num, child_page_num);
    child.set_parent(destination_page_num);

    parent.update_key(old_max, old_node.get_node_max_key(table.pager()));
}

unsafe fn internal_node_split_and_insert_non_root(
    table: &mut Table,
    parent_page_num: u32,
    child_page_num: u32,
) {
    let old_page_num = parent_page_num;
    let old_node = table.pager().page(old_page_num);
    let old_node = InternalNode::new(old_node);
    let old_max = old_node.get_node_max_key(table.pager());

    let child = table.pager().page(child_page_num);
    let child = InternalNode::new(child);
    let child_max = child.get_node_max_key(table.pager());

    let new_page_num = table.pager().get_unused_page_num();

    let old_node_parent = old_node.get_parent();
    let parent = table.pager().page(old_node_parent);

    let new_node = table.pager().page(new_page_num);
    let new_node = InternalNode::new(new_node);
    new_node.initialize();

    let mut cur_page_num = old_node.get_right_child();
    let mut cur = table.pager().page(cur_page_num);

    // First put right child into new node and set right child of old node to invalid page number
    internal_node_insert(table, new_page_num, cur_page_num);
    std::ptr::write(node_parent(cur) as *mut u32, new_page_num);
    old_node.set_right_child(INVALID_PAGE_NUM);

    // For each key until you get to the middle key, move the key and the child to the new node
    for i in (INTERNAL_NODE_MAX_CELLS / 2 + 1..INTERNAL_NODE_MAX_CELLS).rev() {
        cur_page_num = old_node.get_child(i as u32);
        cur = table.pager().page(cur_page_num);

        internal_node_insert(table, new_page_num, cur_page_num);
        std::ptr::write(node_parent(cur) as *mut u32, new_page_num);

        let old_num_keys_value = old_node.get_num_keys();
        old_node.set_num_keys(old_num_keys_value - 1);
    }

    // Set child before middle key, which is now the highest key, to be node's right child and decrement number of keys
    let old_num_keys_value = old_node.get_num_keys();
    let new_right_child = old_node.get_child(old_num_keys_value - 1);
    old_node.set_right_child(new_right_child);
    old_node.set_num_keys(old_num_keys_value - 1);

    // Determine which of the two nodes after the split should contain the child to be inserted and insert the child
    let max_after_split = old_node.get_node_max_key(table.pager());
    let destination_page_num = if child_max < max_after_split {
        old_page_num
    } else {
        new_page_num
    };

    internal_node_insert(table, destination_page_num, child_page_num);
    child.set_parent(destination_page_num);

    let parent = InternalNode::new(parent);
    update_internal_node_key(&parent, old_max, old_node.get_node_max_key(table.pager()));

    let old_node_parent = old_node.get_parent();
    internal_node_insert(table, old_node_parent, new_page_num);
    new_node.set_parent(old_node_parent);
}

pub unsafe fn internal_node_insert(table: &mut Table, parent_page_num: u32, child_page_num: u32) {
    let parent = table.pager().page(parent_page_num);
    let parent = InternalNode::new(parent);
    let child = table.pager().page(child_page_num);
    let child_max_key = get_node_max_key(table.pager(), child);
    let index = parent.find_child(child_max_key);

    let original_num_keys = parent.get_num_keys();
    if original_num_keys >= INTERNAL_NODE_MAX_CELLS as u32 {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    let right_child_page_num = parent.get_right_child();
    if right_child_page_num == INVALID_PAGE_NUM {
        // empty internal node. add to right child
        parent.set_right_child(child_page_num);
        return;
    }

    let right_child = table.pager().page(right_child_page_num);
    parent.set_num_keys(original_num_keys + 1);

    if child_max_key > get_node_max_key(table.pager(), right_child) {
        // Replace right child
        parent.set_child(original_num_keys, right_child_page_num);
        parent.set_key(
            original_num_keys,
            get_node_max_key(table.pager(), right_child),
        );
        parent.set_right_child(child_page_num);
    } else {
        // Add to new cell
        for i in (index + 1..=original_num_keys).rev() {
            let src = parent.cell(i - 1);
            let dest = parent.cell(i);
            copy_internal_cell(src, dest);
        }

        parent.set_child(index, child_page_num);
        parent.set_key(index, child_max_key);
    }
}

pub unsafe fn update_internal_node_key(node: &InternalNode, old_key: u32, new_key: u32) {
    let old_child_index = internal_node_find_child(node, old_key);
    node.set_key(old_child_index, new_key);
}

pub unsafe fn get_internal_node_num_keys(node: *mut u8) -> u32 {
    let node = InternalNode::new(node);
    node.get_num_keys()
}
pub unsafe fn set_internal_node_num_keys(node: *mut u8, num_keys: u32) {
    let node = InternalNode::new(node);
    node.set_num_keys(num_keys);
}

pub unsafe fn get_internal_node_right_child(node: *mut u8) -> u32 {
    let node = InternalNode::new(node);
    node.get_right_child()
}

pub unsafe fn set_internal_node_right_child(node: *mut u8, child: u32) {
    let node = InternalNode::new(node);
    node.set_right_child(child);
}

unsafe fn internal_node_cell(node: *mut u8, cell_num: u32) -> *mut u8 {
    let node = InternalNode::new(node);
    node.cell(cell_num)
}

pub unsafe fn internal_node_child(node: *mut u8, child_num: u32) -> *mut u8 {
    let node = InternalNode::new(node);
    node.child(child_num)
}

pub unsafe fn get_internal_node_key(node: *mut u8, cell_num: u32) -> u32 {
    let node = InternalNode::new(node);
    node.get_key(cell_num)
}
pub unsafe fn set_internal_node_key(node: *mut u8, cell_num: u32, key: u32) {
    let node = InternalNode::new(node);
    node.set_key(cell_num, key);
}

pub unsafe fn initialize_internal_node(node: *mut u8) {
    let node = InternalNode::new(node);
    node.initialize();
}
unsafe fn copy_internal_cell(src: *mut u8, dest: *mut u8) {
    std::ptr::copy_nonoverlapping(src, dest, INTERNAL_NODE_CELL_SIZE);
}

pub struct InternalNode {
    data: *mut u8,
}
impl InternalNode {
    pub fn new(data: *mut u8) -> Self {
        Self { data }
    }
    pub unsafe fn initialize(&self) {
        self.set_node_type(NodeType::Internal);
        self.set_root(false);

        set_internal_node_num_keys(self.data, 0);
        set_internal_node_right_child(self.data, INVALID_PAGE_NUM);
    }

    pub unsafe fn is_root(&self) -> bool {
        let value = std::ptr::read(self.data.add(IS_ROOT_OFFSET) as *const u8);
        return if value == 0 {
            false
        } else if value == 1 {
            true
        } else {
            panic!("invalid value");
        };
    }
    pub unsafe fn set_root(&self, is_root: bool) {
        std::ptr::write(
            self.data.add(IS_ROOT_OFFSET),
            if is_root == true { 1u8 } else { 0u8 },
        );
    }
    unsafe fn _get_node_type(&self) -> NodeType {
        let value = *self.data.add(NODE_TYPE_OFFSET);
        value.into()
    }
    unsafe fn set_node_type(&self, node_type: NodeType) {
        let value = node_type.into();
        std::ptr::write(self.data.add(NODE_TYPE_OFFSET), value);
    }

    pub unsafe fn get_key(&self, cell_num: u32) -> u32 {
        std::ptr::read(
            internal_node_cell(self.data, cell_num).add(INTERNAL_NODE_CHILD_SIZE) as *const u32,
        )
    }
    pub unsafe fn set_key(&self, cell_num: u32, key: u32) {
        std::ptr::write(
            internal_node_cell(self.data, cell_num).add(INTERNAL_NODE_CHILD_SIZE) as *mut u32,
            key,
        );
    }

    pub unsafe fn cell(&self, cell_num: u32) -> *mut u8 {
        self.data
            .add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
    }

    pub unsafe fn get_num_keys(&self) -> u32 {
        std::ptr::read(self.data.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *const u32)
    }
    pub unsafe fn set_num_keys(&self, num_keys: u32) {
        std::ptr::write(
            self.data.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32,
            num_keys,
        );
    }

    pub unsafe fn get_right_child(&self) -> u32 {
        std::ptr::read(self.data.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *const u32)
    }

    pub unsafe fn set_right_child(&self, child: u32) {
        std::ptr::write(
            self.data.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32,
            child,
        );
    }

    unsafe fn right_child(&self) -> *mut u8 {
        self.data.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET)
    }

    unsafe fn get_node_max_key(&self, pager: &mut Pager) -> u32 {
        let right_child_page_num = self.get_right_child();
        let right_child = pager.page(right_child_page_num);
        get_node_max_key(pager, right_child)
    }

    pub unsafe fn find_child(&self, key: u32) -> u32 {
        let num_keys = self.get_num_keys();

        let mut min_index = 0u32;
        let mut max_index = num_keys;

        while min_index != max_index {
            let index = (min_index + max_index) / 2;
            let key_to_right = self.get_key(index);
            if key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        min_index
    }

    pub unsafe fn child(&self, child_num: u32) -> *mut u8 {
        let num_keys = self.get_num_keys();

        if child_num > num_keys {
            println!(
                "Tried to access child num {} > num_keys {}",
                child_num, num_keys
            );
            exit(EXIT_FAILURE);
        } else if child_num == num_keys {
            let right_child = self.get_right_child();
            if right_child == INVALID_PAGE_NUM {
                println!("Tried to access child of node, but was invalid page");
                exit(EXIT_FAILURE);
            }

            self.right_child()
        } else {
            // child_num < num_keys
            let child = self.cell(child_num);
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

    pub unsafe fn set_child(&self, cell: u32, child: u32) {
        std::ptr::write(self.child(cell) as *mut u32, child);
    }
    pub unsafe fn get_child(&self, cell: u32) -> u32 {
        std::ptr::read(self.child(cell) as *const u32)
    }

    pub unsafe fn update_key(&self, old_key: u32, new_key: u32) {
        let old_child_index = self.find_child(old_key);
        self.set_key(old_child_index, new_key);
    }

    pub unsafe fn get_parent(&self) -> u32 {
        std::ptr::read(self.data.add(PARENT_POINTER_OFFSET) as *const u32)
    }
    pub unsafe fn set_parent(&self, parent: u32) {
        std::ptr::write(self.data.add(PARENT_POINTER_OFFSET) as *mut u32, parent);
    }
}
