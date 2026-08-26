use crate::protocol::Stat;
use crate::znode::ZNode;
use std::collections::HashMap;
use std::time::SystemTime;

pub struct KeeperStorage {
    root: ZNode,
}

impl KeeperStorage {
    pub fn new() -> Self {
        KeeperStorage {
            // The root node is basically a sentinel.
            // We can put all values as zeros as defaults here.
            root: ZNode {
                data: Vec::new(),
                children: HashMap::new(),
                stat: Stat::default(),
            },
        }
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

        if parent.children.contains_key(child_name) {
            return Err("Node already exists");
        }

        // TODO: We don't yet have the czxid counter yet. We keep it as a
        // placeholder for now. czxid is the global counter on KeeperServer.
        // Every write operation increments it. Once that is built, we need
        // to change this.
        let czxid = 0i64;

        // TODO: In a real distributed setup, we can't take local time. We need
        // to agree on the time across nodes also! For v1, this is fine. But we
        // need to revisit this once we are starting work for v2.
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let newnode = ZNode {
            data,
            children: HashMap::new(),
            stat: Stat {
                czxid,
                mzxid: czxid,
                ctime: now,
                mtime: now,
                version: 0,
                cversion: 0,
                aversion: 0,
                ephemeral_owner: 0,
                pzxid: czxid,
            },
        };

        parent.children.insert(child_name.to_string(), newnode);

        parent.stat.cversion += 1;

        // TODO: Need to change this when we add zxid counter
        parent.stat.pzxid = 0;

        Ok(())
    }

    pub fn exists(&self, path: &str) -> bool {
        self.traverse(path).is_some()
    }

    pub fn set(&mut self, path: &str, data: Vec<u8>) -> Result<(), &'static str> {
        match self.traverse_mut(path) {
            Some(node) => {
                // TODO: This needs to change in v2. We can't take local time.
                // For v1, this is fine
                let now = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                node.stat.version += 1;

                // TODO: When zxid counter logic is added, change this.
                node.stat.mzxid = 0;
                node.stat.mtime = now;
                node.data = data;

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

        parent.stat.cversion += 1;

        // TODO: Need to change this when we add zxid counter
        parent.stat.pzxid = 0;

        Ok(())
    }
}
