use crate::protocol::{SessionId, Stat};
use crate::session_expiry_queue::SessionExpiryQueue;
use crate::znode::Node;
use std::collections::{HashMap, HashSet};

/// Every client that connects to our server gets a session.
/// The session is how we track "this client is alive."
/// If the client stops sending requests, eventually its session
/// expires and we clean up after it — specifically, we delete any
/// ephemeral nodes it created.
///
/// SessionState is the struct that holds all of that bookkeeping.
/// It needs to answer four questions:
/// 1. Which sessions exist, and what's their timeout?
/// 2. Which ephemeral paths does each session own? →
/// 3. What's the next session ID to hand out?
/// 4. When does each session expire?
pub struct SessionState {
    session_and_timeout: HashMap<SessionId, i64>,
    ephemerals: HashMap<SessionId, HashSet<String>>,
    next_session_id: i64,
    expiry_queue: SessionExpiryQueue,
}

impl SessionState {
    pub fn new(interval_ms: i64) -> Self {
        Self {
            session_and_timeout: HashMap::new(),
            ephemerals: HashMap::new(),
            next_session_id: 1,
            expiry_queue: SessionExpiryQueue::new(interval_ms),
        }
    }

    /// create_session is called during the handshake when a new client connects. It needs to:
    /// 1. Grab the current next_session_id and bump it for next time.
    /// 2. Record this session and its timeout in session_and_timeout.
    /// 3. Touch the expiry queue so the session has an expiry deadline.
    /// 4. Return the new SessionId so the dispatcher can send it back to the client.
    ///
    /// In v2+, this method will be called only on the leader keeper node
    pub fn create_session(&mut self, timeout_ms: i64, now: i64) -> SessionId {
        let id = SessionId(self.next_session_id);
        self.next_session_id += 1;
        self.session_and_timeout.insert(id, timeout_ms);
        self.expiry_queue.touch(id, timeout_ms, now);
        id
    }

    /// touch_session refreshes the session's expiry deadline.
    /// It's called on every request from the client
    pub fn touch_session(&mut self, id: SessionId, now: i64) {
        // If the session id doesn't even exist, we do nothing.
        if let Some(&timeout_ms) = self.session_and_timeout.get(&id) {
            self.expiry_queue.touch(id, timeout_ms, now);
        }
    }

    /// Called when a client creates an ephemeral znode.
    pub fn add_ephemeral(&mut self, id: SessionId, path: String) {
        self.ephemerals.entry(id).or_default().insert(path);
    }

    /// Called when a client explicitly deletes an ephemeral node
    pub fn remove_ephemeral(&mut self, id: SessionId, path: &str) {
        if let Some(paths) = self.ephemerals.get_mut(&id) {
            paths.remove(path);
            if paths.is_empty() {
                self.ephemerals.remove(&id);
            }
        }
    }

    /// close_session is called when a session ends- either the client sends
    /// OpCode::Close or the background task detects it expired. It needs to
    /// clean up everything this session owned and return the list of ephemeral
    /// paths so the caller can delete those znodes.
    ///
    /// NOTE: The znodes are not deleted here. The caller does that
    pub fn close_session(&mut self, id: SessionId) -> Vec<String> {
        self.session_and_timeout.remove(&id);
        self.expiry_queue.remove(id);
        self.ephemerals
            .remove(&id)
            .map(|paths| paths.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn get_expired(&self, now: i64) -> Vec<SessionId> {
        self.expiry_queue.get_expired(now)
    }

    pub fn is_alive(&self, id: SessionId) -> bool {
        self.session_and_timeout.contains_key(&id)
    }
}

pub struct KeeperStorage {
    map: HashMap<String, Node>,

    // Ideally, we would use an AtomicI64.
    // But KeeperStorage is always used inside a RwLock.
    // So, it's safe to have the zxid as a normal int
    last_zxid: i64,

    pub session_state: SessionState,
}

impl KeeperStorage {
    pub fn new(interval_ms: i64) -> Self {
        let mut map = HashMap::new();
        map.insert(
            "/".to_string(),
            Node {
                data: Vec::new(),
                children: HashSet::new(),
                stat: Stat::default(),
            },
        );
        KeeperStorage {
            map,
            last_zxid: 0,
            session_state: SessionState::new(interval_ms),
        }
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

    #[allow(clippy::collapsible_if)]
    pub fn create(
        &mut self,
        path: &str,
        data: Vec<u8>,
        timestamp: i64,
        session_id: SessionId,
        flags: i32,
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

        if let Some(parent) = self.map.get(parent_path) {
            if parent.stat.ephemeral_owner != SessionId(0) {
                return Err("Cannot create child under ephemeral node");
            }
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
                ephemeral_owner: if flags & 1 != 0 {
                    session_id
                } else {
                    SessionId(0)
                },
                pzxid: self.last_zxid,
            },
        };

        // Mutation 1: insert the new node
        self.map.insert(path.to_string(), new_node);

        if flags & 1 != 0 {
            self.session_state
                .add_ephemeral(session_id, path.to_string());
        }

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
