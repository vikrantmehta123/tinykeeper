use crate::protocol::Stat;
use crate::znode::Node;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

pub struct KeeperStorage {
    map: HashMap<String, Node>,
}

impl KeeperStorage {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        map.insert(
            "/".to_string(),
            Node {
                data: Vec::new(),
                children: HashSet::new(),
                stat: Stat::default(),
            },
        );
        KeeperStorage { map }
    }

    pub fn create(&mut self, path: &str, data: Vec<u8>) -> Result<(), &'static str> {
        let (parent_path, child_name) = match path.rsplit_once("/") {
            Some((p, c)) => (p, c),
            None => return Err("Invalid path format"),
        };

        if child_name.is_empty() {
            return Err("Cannot create root node");
        }

        let parent_path = if parent_path.is_empty() {
            "/"
        } else {
            parent_path
        };

        if !self.map.contains_key(parent_path) {
            return Err("Parent node does not exist");
        }

        if self.map.contains_key(path) {
            return Err("Node already exists");
        }

        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // TODO: Placeholder zxid. Fix it once you have atomic counter
        let czxid = 0i64;

        let new_node = Node {
            data,
            children: HashSet::new(),
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

        // Mutation 1: insert the new node
        self.map.insert(path.to_string(), new_node);

        // Mutation 2: update the parent
        let parent = self.map.get_mut(parent_path).unwrap();
        parent.children.insert(child_name.to_string());
        parent.stat.cversion += 1;
        parent.stat.pzxid = 0; // TODO: Update this once atomic counting zxid is there

        Ok(())
    }

    pub fn traverse(&self, path: &str) -> Option<&Node> {
        self.map.get(path)
    }

    pub fn traverse_mut(&mut self, path: &str) -> Option<&mut Node> {
        self.map.get_mut(path)
    }

    pub fn exists(&self, path: &str) -> bool {
        self.map.contains_key(path)
    }

    pub fn set(&mut self, path: &str, data: Vec<u8>) -> Result<(), &'static str> {
        match self.map.get_mut(path) {
            Some(node) => {
                // TODO: This needs to change in v2. We can't take local time.
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
        let (parent_path, child_name) = match path.rsplit_once("/") {
            Some((p, c)) => (p, c),
            None => return Err("Invalid path format"),
        };

        if child_name.is_empty() {
            return Err("Cannot delete root node");
        }

        let parent_path = if parent_path.is_empty() {
            "/"
        } else {
            parent_path
        };

        match self.map.get(path) {
            Some(node) => {
                if !node.children.is_empty() {
                    return Err("Node has children");
                }
            }
            None => return Err("Node not found"),
        }

        // Mutation 1: remove the node
        self.map.remove(path);

        // Mutation 2: update the parent
        let parent = self.map.get_mut(parent_path).unwrap();
        parent.children.remove(child_name);
        parent.stat.cversion += 1;
        // TODO: Need to change this when we add zxid counter
        parent.stat.pzxid = 0;

        Ok(())
    }
}
