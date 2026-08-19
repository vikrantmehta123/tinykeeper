use crate::znode::ZNode;

pub struct KeeperStorage {
    root: ZNode,
}

impl KeeperStorage {
    pub fn new() -> Self {
        KeeperStorage { root: ZNode::new() }
    }

    pub fn traverse(&self, path: &str) -> Option<&ZNode> {
        let segments = path.split("/").filter(|s| !s.is_empty());

        let mut current_root = &self.root;
        for part in segments {
            let child = current_root.children.get(part)?;
            current_root = child;
        }
        Some(current_root)
    }

    pub fn traverse_mut(&mut self, path: &str) -> Option<&mut ZNode> {
        let segments = path.split("/").filter(|s| !s.is_empty());

        let mut current_root = &mut self.root;
        for part in segments {
            let child = current_root.children.get_mut(part)?;
            current_root = child;
        }

        Some(current_root)
    }

    pub fn create(&mut self, path: &str, data: Vec<u8>) -> Result<(), &'static str> {
        let (parent_path, child_name) = match path.rsplit_once("/") {
            Some((p, c)) => (p, c),
            None => return Err("Invalid path format. No child found"),
        };

        // Prevent trying to recreate the root node (e.g., path == "/")
        if child_name.is_empty() {
            return Err("Cannot create root node");
        }

        let parent = match self.traverse_mut(parent_path) {
            Some(p) => p,
            None => return Err("Parent node does not exist"),
        };

        let mut newnode = ZNode::new();
        newnode.data = data;

        if parent.children.contains_key(child_name) {
            return Err("Node already exists");
        }

        parent.children.insert(child_name.to_string(), newnode);

        Ok(())
    }

    pub fn get(&self, path: &str) -> Result<Vec<u8>, &'static str> {
        match self.traverse(path) {
            Some(node) => {
                // If found, we must .clone() the data because our function
                // signature promises to return an owned Vec<u8>, but `traverse`
                // only gives us a borrowed reference (&ZNode).
                Ok(node.data.clone())
            }
            None => Err("Node not found"),
        }
    }

    pub fn set(&mut self, path: &str, data: Vec<u8>) -> Result<(), &'static str> {
        match self.traverse_mut(path) {
            Some(node) => {
                node.data = data;
                // It is very important in distributed systems to increment the version
                // whenever a node changes so clients know an update happened!
                // This is for Optimistic Concurrency Control.
                // TODO: Later, revisit this and understand this better
                node.version += 1;

                Ok(())
            }
            None => Err("Node not found"),
        }
    }

    pub fn delete(&mut self, path: &str) -> Result<(), &'static str> {
        // Split the path just like in create
        let (parent_path, child_name) = match path.rsplit_once("/") {
            Some((p, c)) => (p, c),
            None => return Err("Invalid path format. No child found"),
        };

        if child_name.is_empty() {
            return Err("Cannot delete root node");
        }

        // Find the mutable parent node
        let parent = match self.traverse_mut(parent_path) {
            Some(p) => p,
            None => return Err("Parent node does not exist"),
        };

        // Remove the child from the parent's HashMap
        // The .remove() method returns None if the key wasn't in the map
        // .remove() will delete the node's children's also! Ideally,
        // we don't want this. Ideally, we want to prevent the deletion
        // for a node that still has children.
        if parent.children.remove(child_name).is_none() {
            return Err("Node not found");
        }

        Ok(())
    }
}
