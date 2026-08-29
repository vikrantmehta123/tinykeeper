use crate::protocol::{SessionId, Stat};
use crate::znode::Node;
use std::collections::{HashMap, HashSet};

pub struct KeeperStorage {
    map: HashMap<String, Node>,

    // Ideally, we would use an AtomicI64.
    // But KeeperStorage is always used inside a RwLock.
    // So, it's safe to have the zxid as a normal int
    last_zxid: i64,
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
        KeeperStorage { map, last_zxid: 0 }
    }

    pub fn last_zxid(&self) -> i64 {
        self.last_zxid
    }

    pub fn next_zxid(&mut self) -> i64 {
        self.last_zxid += 1;
        self.last_zxid
    }

    pub fn set_last_zxid(&mut self, zxid: i64) {
        self.last_zxid = zxid;
    }

    pub fn create(
        &mut self,
        path: &str,
        data: Vec<u8>,
        timestamp: i64,
    ) -> Result<(), &'static str> {
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

        let new_node = Node {
            data,
            children: HashSet::new(),
            stat: Stat {
                czxid: self.last_zxid,
                mzxid: self.last_zxid,
                ctime: timestamp,
                mtime: timestamp,
                version: 0,
                cversion: 0,
                aversion: 0,
                ephemeral_owner: SessionId(0),
                pzxid: self.last_zxid,
            },
        };

        // Mutation 1: insert the new node
        self.map.insert(path.to_string(), new_node);

        // Mutation 2: update the parent
        let parent = self.map.get_mut(parent_path).unwrap();
        parent.children.insert(child_name.to_string());
        parent.stat.cversion += 1;
        parent.stat.pzxid = self.last_zxid;

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

    pub fn set(&mut self, path: &str, data: Vec<u8>, timestamp: i64) -> Result<(), &'static str> {
        match self.map.get_mut(path) {
            Some(node) => {
                node.stat.version += 1;

                node.stat.mzxid = self.last_zxid;
                node.stat.mtime = timestamp;
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

        parent.stat.pzxid = self.last_zxid;

        Ok(())
    }
}
