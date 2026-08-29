use tokio::sync::{Mutex, RwLock};

use crate::changelog::WalStore;
use crate::protocol::*;
use crate::storage::KeeperStorage;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::SystemTime;

#[derive(Serialize, Deserialize)]
enum WalOperation {
    Create {
        path: String,
        data: Vec<u8>,
        timestamp: i64,
    },
    Set {
        path: String,
        data: Vec<u8>,
        timestamp: i64,
    },
    Delete {
        path: String,
    },
}

impl WalOperation {
    fn serialize(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialization should not fail")
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}

pub struct KeeperServer {
    storage: RwLock<KeeperStorage>,
    wal: Mutex<WalStore>,
}

impl KeeperServer {
    pub async fn new(wal_dir: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut storage = KeeperStorage::new(500);

        let wal = WalStore::open(wal_dir, 50 * 1024 * 1024).await?;

        wal.replay(|index, _term, payload| {
            storage.set_last_zxid(index as i64);
            if let Some(op) = WalOperation::deserialize(&payload) {
                match op {
                    WalOperation::Create {
                        path,
                        data,
                        timestamp,
                    } => {
                        let _ = storage.create(&path, data, timestamp, SessionId(0), 0);
                    }
                    WalOperation::Set {
                        path,
                        data,
                        timestamp,
                    } => {
                        let _ = storage.set(&path, data, timestamp);
                    }
                    WalOperation::Delete { path } => {
                        let _ = storage.delete(&path);
                    }
                }
            }
        })
        .await?;

        Ok(KeeperServer {
            storage: RwLock::new(storage),
            wal: Mutex::new(wal),
        })
    }

    pub async fn create_session(&self, timeout_ms: i64) -> SessionId {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.storage
            .write()
            .await
            .session_state
            .create_session(timeout_ms, now)
    }

    pub async fn close_session(&self, session_id: SessionId) {
        let mut tree = self.storage.write().await;
        let paths = tree.session_state.close_session(session_id);
        for path in &paths {
            let _ = tree.delete(path);
        }
    }

    pub async fn get_expired_sessions(&self) -> Vec<SessionId> {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.storage.read().await.session_state.get_expired(now)
    }

    pub async fn apply(&self, payload: &[u8], session_id: SessionId) -> Vec<u8> {
        if payload.len() < 8 {
            println!("Message too short to be a standard request");
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: 0,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        }

        // TODO: Review the locking system end-to-end. Currently, we are holding onto the
        // locks for long time. And that's going to add to the cost
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.storage
            .write()
            .await
            .session_state
            .touch_session(session_id, now);

        let mut buf = payload;
        let header = RequestHeader::from_bytes(&mut buf).expect("unknown opcode");
        println!(
            "Parsed Header -> xid: {}, OpCode: {:?}",
            header.xid, header.opcode
        );
        match header.opcode {
            OpCode::Create => self.handle_create(header, &mut buf, session_id).await,
            OpCode::Get => self.handle_get(header, &mut buf).await,
            OpCode::Set => self.handle_set(header, &mut buf).await,
            OpCode::Remove => self.handle_delete(header, &mut buf).await,
            OpCode::Heartbeat => {
                let tree = self.storage.read().await;
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                reply_header.to_bytes()
            }
            OpCode::SimpleList => self.handle_get_children_simple(header, &mut buf).await,
            OpCode::List => self.handle_get_children(header, &mut buf).await,
            OpCode::Exists => self.handle_exists(header, &mut buf).await,
            OpCode::Close => {
                let mut tree = self.storage.write().await;
                let paths = tree.session_state.close_session(session_id);
                for path in &paths {
                    let _ = tree.delete(path);
                }
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                reply_header.to_bytes()
            }
            _ => {
                println!("Received unimplemented OpCode: {:?}", header.opcode);
                let tree = self.storage.read().await;
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::BadArguments,
                };
                reply_header.to_bytes()
            }
        }
    }
    async fn handle_exists(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = ExistsRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };

        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                let res = ExistsResponse { stat: &node.stat };
                let mut payload = reply_header.to_bytes();
                payload.extend(res.to_bytes(node.data.len() as i32, node.children.len() as i32));
                payload
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_get_children(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = GetChildrenRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };
        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };

                let mut children: Vec<&String> = node.children.iter().collect();
                children.sort();

                let mut payload = reply_header.to_bytes();

                let res = GetChildrenResponse {
                    children: &children,
                };
                payload.extend(res.to_bytes());
                payload.extend(
                    node.stat
                        .to_bytes(node.data.len() as i32, node.children.len() as i32),
                );
                payload
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_get_children_simple(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = GetChildrenRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };
        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };

                let mut children: Vec<&String> = node.children.iter().collect();
                children.sort();

                let mut payload = reply_header.to_bytes();

                let res = GetChildrenResponse {
                    children: &children,
                };
                payload.extend(res.to_bytes());
                payload
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }
    async fn handle_create(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> Vec<u8> {
        let Some(req) = CreateRequest::from_bytes(buf) else {
            println!("Failed to parse CreateRequest payload!");
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };

        let mut tree = self.storage.write().await;

        if tree.exists(req.path) {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::NodeExists,
            };
            return reply_header.to_bytes();
        }

        let (parent_path, child_name) = match req.path.rsplit_once("/") {
            Some((p, c)) => (p, c),
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::BadArguments,
                };
                return reply_header.to_bytes();
            }
        };

        if child_name.is_empty() {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        }

        let parent_path = if parent_path.is_empty() {
            "/"
        } else {
            parent_path
        };

        if !tree.exists(parent_path) {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::NoNode,
            };
            return reply_header.to_bytes();
        }

        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let op = WalOperation::Create {
            path: req.path.to_string(),
            data: req.data.to_vec(),
            timestamp: now,
        };
        let payload = op.serialize();

        let zxid = tree.next_zxid();

        let mut wal = self.wal.lock().await;
        wal.append(1, zxid as u64, 0, 0, &payload);
        if let Err(e) = wal.flush().await {
            println!("Failed to write to WAL: {}", e);
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        }
        drop(wal);

        let _ = tree.create(req.path, req.data.to_vec(), now, session_id, req.flags);

        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid: tree.last_zxid(),
            err: ErrorCode::Ok,
        };
        let create_res = CreateResponse { path: req.path };
        let mut response_payload = reply_header.to_bytes();
        response_payload.extend(create_res.to_bytes());
        response_payload
    }

    async fn handle_get(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = GetDataRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };

        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                let get_res = GetDataResponse {
                    data: &node.data,
                    stat: &node.stat,
                };
                let mut payload = reply_header.to_bytes();
                payload.extend(get_res.to_bytes(node.children.len() as i32));
                payload
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_set(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = SetDataRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };

        // Brute Force: Get the tree lock early.
        // TODO: In future, this should be pipelined
        let mut tree = self.storage.write().await;

        let current_version = match tree.traverse(req.path) {
            Some(node) => node.stat.version,
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                return reply_header.to_bytes();
            }
        };

        if req.version != -1 && current_version != req.version {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadVersion,
            };
            return reply_header.to_bytes();
        }

        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let op = WalOperation::Set {
            path: req.path.to_string(),
            data: req.data.to_vec(),
            timestamp: now,
        };
        let payload = op.serialize();

        let zxid = tree.next_zxid();

        let mut wal = self.wal.lock().await;
        wal.append(1, zxid as u64, 0, 0, &payload);
        if let Err(e) = wal.flush().await {
            println!("Failed to write to WAL: {}", e);
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        }
        drop(wal);

        let _ = tree.set(req.path, req.data.to_vec(), now);

        let node = tree.traverse(req.path).unwrap();
        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid: tree.last_zxid(),
            err: ErrorCode::Ok,
        };
        let set_res = SetDataResponse { stat: &node.stat };
        let mut payload = reply_header.to_bytes();
        payload.extend(set_res.to_bytes(node.data.len() as i32, node.children.len() as i32));
        payload
    }

    async fn handle_delete(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = DeleteRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        };

        // Brute Force: Get the tree lock early.
        // TODO: In future, this should be pipelined
        let mut tree = self.storage.write().await;

        let node = match tree.traverse(req.path) {
            Some(node) => node,
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                return reply_header.to_bytes();
            }
        };

        if req.version != -1 && node.stat.version != req.version {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadVersion,
            };
            return reply_header.to_bytes();
        }

        if !node.children.is_empty() {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::NotEmpty,
            };
            return reply_header.to_bytes();
        }

        let op = WalOperation::Delete {
            path: req.path.to_string(),
        };
        let payload = op.serialize();

        let zxid = tree.next_zxid();

        let mut wal = self.wal.lock().await;
        wal.append(1, zxid as u64, 0, 0, &payload);
        if let Err(e) = wal.flush().await {
            println!("Failed to write to WAL: {}", e);
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return reply_header.to_bytes();
        }
        drop(wal);

        let owner = tree.traverse(req.path).unwrap().stat.ephemeral_owner;
        let _ = tree.delete(req.path);
        if owner != SessionId(0) {
            tree.session_state.remove_ephemeral(owner, req.path);
        }

        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid: tree.last_zxid(),
            err: ErrorCode::Ok,
        };
        let mut payload = reply_header.to_bytes();
        payload.extend(EmptyResponse.to_bytes());
        payload
    }
}
