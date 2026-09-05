use tokio::sync::{Mutex, RwLock};

use crate::changelog::WalStore;
use crate::protocol::*;
use crate::storage::KeeperStorage;
use crate::watch_state::{ApplyResult, WatchType};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::SystemTime;

#[derive(Serialize, Deserialize)]
enum MultiWalOperation {
    Create {
        path: String,
        data: Vec<u8>,
        flags: i32,
    },
    Set {
        path: String,
        data: Vec<u8>,
    },
    Delete {
        path: String,
    },
}

#[derive(Serialize, Deserialize)]
enum WalOperation {
    Create {
        path: String,
        data: Vec<u8>,
        timestamp: i64,
        flags: i32,
        session_id: i64,
    },
    Set {
        path: String,
        data: Vec<u8>,
        timestamp: i64,
    },
    Delete {
        path: String,
    },

    CreateSession {
        session_id: i64,
        timeout_ms: i64,
    },
    CloseSession {
        session_id: i64,
    },
    Multi {
        operations: Vec<MultiWalOperation>,
        timestamp: i64,
        session_id: i64,
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
            let op = WalOperation::deserialize(&payload).expect("corrupt WAL entry during replay");
            match op {
                WalOperation::Create {
                    path,
                    data,
                    timestamp,
                    flags,
                    session_id,
                } => {
                    storage
                        .create(&path, data, timestamp, SessionId(session_id), flags)
                        .expect("WAL replay of Create failed: log is inconsistent with state");
                }
                WalOperation::Set {
                    path,
                    data,
                    timestamp,
                } => {
                    storage
                        .set(&path, data, timestamp)
                        .expect("WAL replay of Set failed: log is inconsistent with state");
                }
                WalOperation::Delete { path } => {
                    storage
                        .delete(&path)
                        .expect("WAL replay of Delete failed: log is inconsistent with state");
                }
                WalOperation::CreateSession {
                    session_id,
                    timeout_ms,
                } => {
                    storage
                        .session_state
                        .restore_session(SessionId(session_id), timeout_ms);
                }
                WalOperation::CloseSession { session_id } => {
                    let paths = storage.session_state.close_session(SessionId(session_id));
                    for path in &paths {
                        storage
                            .delete(path)
                            .expect("WAL replay of CloseSession cleanup failed");
                    }
                }
                WalOperation::Multi {
                    operations,
                    timestamp,
                    session_id,
                } => {
                    for operation in operations {
                        match operation {
                            MultiWalOperation::Create { path, data, flags } => {
                                storage
                                    .create(&path, data, timestamp, SessionId(session_id), flags)
                                    .expect("WAL replay of Multi Create failed");
                            }
                            MultiWalOperation::Set { path, data } => {
                                storage
                                    .set(&path, data, timestamp)
                                    .expect("WAL replay of Multi Set failed");
                            }
                            MultiWalOperation::Delete { path } => {
                                let owner = storage
                                    .traverse(&path)
                                    .expect("WAL replay of Multi Delete: node missing")
                                    .stat
                                    .ephemeral_owner;

                                storage
                                    .delete(&path)
                                    .expect("WAL replay of Multi Delete failed");

                                if owner != SessionId(0) {
                                    storage.session_state.remove_ephemeral(owner, &path);
                                }
                            }
                        }
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
        let mut tree = self.storage.write().await;
        let session_id = tree.session_state.create_session(timeout_ms, now);

        let op = WalOperation::CreateSession {
            session_id: session_id.0,
            timeout_ms,
        };
        let zxid = tree.next_zxid();
        let mut wal = self.wal.lock().await;
        wal.append(1, zxid as u64, 0, 0, &op.serialize());
        let _ = wal.flush().await;

        session_id
    }

    pub async fn close_session(&self, session_id: SessionId) {
        let mut tree = self.storage.write().await;

        let op = WalOperation::CloseSession {
            session_id: session_id.0,
        };
        let zxid = tree.next_zxid();
        let mut wal = self.wal.lock().await;
        wal.append(1, zxid as u64, 0, 0, &op.serialize());
        let _ = wal.flush().await;
        drop(wal);

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

    pub async fn apply(&self, payload: &[u8], session_id: SessionId) -> ApplyResult {
        if payload.len() < 8 {
            println!("Message too short to be a standard request");
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: 0,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
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
            OpCode::Get => self.handle_get(header, &mut buf, session_id).await,
            OpCode::Set => self.handle_set(header, &mut buf, session_id).await,
            OpCode::Remove => self.handle_delete(header, &mut buf, session_id).await,
            OpCode::Heartbeat => {
                let tree = self.storage.read().await;
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
            OpCode::SimpleList => {
                self.handle_get_children_simple(header, &mut buf, session_id)
                    .await
            }
            OpCode::List => self.handle_get_children(header, &mut buf, session_id).await,
            OpCode::Exists => self.handle_exists(header, &mut buf, session_id).await,
            OpCode::Close => {
                let mut tree = self.storage.write().await;
                let paths = tree.session_state.close_session(session_id);
                for path in &paths {
                    let _ = tree.delete(path);
                }
                tree.watch_state.clear(session_id);
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
            OpCode::Multi => self.handle_multi(header, &mut buf, session_id).await,
            _ => {
                println!("Received unimplemented OpCode: {:?}", header.opcode);
                let tree = self.storage.read().await;
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::BadArguments,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
        }
    }

    async fn handle_multi(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = MultiRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;

            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;
        let mut staged = tree.clone();

        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_millis() as i64;

        let zxid = staged.next_zxid();
        let op_count = req.ops.len();
        let mut responses = Vec::with_capacity(op_count);
        let mut wal_operations = Vec::with_capacity(op_count);
        let mut watch_events = Vec::new();
        for op in req.ops {
            let result = match op {
                MultiOp::Check(req) => match staged.traverse(req.path) {
                    None => Err(ErrorCode::NoNode),
                    Some(node) => {
                        if req.version != -1 && req.version != node.stat.version {
                            Err(ErrorCode::BadVersion)
                        } else {
                            Ok(MultiOpResponse::Check)
                        }
                    }
                },
                MultiOp::Set(req) => match staged.traverse(req.path) {
                    None => Err(ErrorCode::NoNode),
                    Some(node) => {
                        if req.version != -1 && req.version != node.stat.version {
                            Err(ErrorCode::BadVersion)
                        } else {
                            staged
                                .set(req.path, req.data.to_vec(), timestamp)
                                .expect("node was validated to exist");

                            watch_events
                                .extend(staged.watch_state.fire(req.path, WatchEventType::Changed));
                            wal_operations.push(MultiWalOperation::Set {
                                path: req.path.to_string(),
                                data: req.data.to_vec(),
                            });

                            let updated = staged
                                .traverse(req.path)
                                .expect("Set does not remove the node");

                            Ok(MultiOpResponse::Set {
                                stat: updated.stat,
                                data_length: updated.data.len() as i32,
                                num_children: updated.children.len() as i32,
                            })
                        }
                    }
                },
                MultiOp::Create(req) => {
                    match staged.create(
                        req.path,
                        req.data.to_vec(),
                        timestamp,
                        session_id,
                        req.flags,
                    ) {
                        Ok(path) => {
                            watch_events
                                .extend(staged.watch_state.fire(&path, WatchEventType::Created));
                            wal_operations.push(MultiWalOperation::Create {
                                path: req.path.to_string(), // Original path, before sequential suffix.
                                data: req.data.to_vec(),
                                flags: req.flags,
                            });

                            Ok(MultiOpResponse::Create { path })
                        }
                        Err(error) => Err(match error {
                            "Parent node does not exist" => ErrorCode::NoNode,
                            "Node already exists" => ErrorCode::NodeExists,
                            "Cannot create child under ephemeral node" => {
                                ErrorCode::NoChildrenForEphemerals
                            }
                            "Invalid path format" | "Cannot create root node" => {
                                ErrorCode::BadArguments
                            }
                            _ => ErrorCode::SystemError,
                        }),
                    }
                }
                MultiOp::Delete(req) => match staged.traverse(req.path) {
                    None => Err(ErrorCode::NoNode),
                    Some(node) => {
                        if req.version != -1 && req.version != node.stat.version {
                            Err(ErrorCode::BadVersion)
                        } else if req.path == "/" {
                            Err(ErrorCode::BadArguments)
                        } else if !node.children.is_empty() {
                            Err(ErrorCode::NotEmpty)
                        } else {
                            let owner = node.stat.ephemeral_owner;

                            staged
                                .delete(req.path)
                                .expect("delete conditions were validated");

                            if owner != SessionId(0) {
                                staged.session_state.remove_ephemeral(owner, req.path);
                            }

                            watch_events
                                .extend(staged.watch_state.fire(req.path, WatchEventType::Deleted));
                            wal_operations.push(MultiWalOperation::Delete {
                                path: req.path.to_string(),
                            });

                            Ok(MultiOpResponse::Delete)
                        }
                    }
                },
            };

            match result {
                Ok(response) => responses.push(response),
                Err(error) => {
                    // Earlier operations succeeded on staged, but are now rolled back.
                    // Overwriting earlier pushed responses.
                    responses.fill(MultiOpResponse::Error(ErrorCode::Ok));

                    // The operation that failed.
                    responses.push(MultiOpResponse::Error(error));

                    // Remaining operations were never executed.
                    responses.resize(
                        op_count,
                        MultiOpResponse::Error(ErrorCode::RuntimeInconsistency),
                    );

                    let reply_header = ReplyHeader {
                        xid: header.xid,
                        zxid: tree.last_zxid(),
                        err: ErrorCode::Ok,
                    };

                    let mut response = reply_header.to_bytes();
                    response.extend(MultiResponse { responses }.to_bytes());

                    return ApplyResult {
                        response,
                        watch_events: vec![],
                    };
                }
            }
        }

        // `multi` was successful. Write WAL, swap tree and staged, and return response
        let operation = WalOperation::Multi {
            operations: wal_operations,
            timestamp,
            session_id: session_id.0,
        };
        let payload = operation.serialize();
        let mut wal = self.wal.lock().await;
        wal.append(1, zxid as u64, 0, 0, &payload);

        if let Err(error) = wal.flush().await {
            todo!("Handle uncertain WAL persistence or rotation failure: {error}");
        }

        *tree = staged;
        drop(wal);

        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid,
            err: ErrorCode::Ok,
        };

        let mut response = reply_header.to_bytes();
        response.extend(MultiResponse { responses }.to_bytes());

        ApplyResult {
            response,
            watch_events,
        }
    }
    async fn handle_exists(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = ExistsRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        if req.watch {
            tree.watch_state
                .register(session_id, req.path.to_string(), WatchType::Watch);
        }

        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };
                let res = ExistsResponse { stat: &node.stat };
                let mut response = reply_header.to_bytes();
                response.extend(res.to_bytes(node.data.len() as i32, node.children.len() as i32));
                ApplyResult {
                    response,
                    watch_events: vec![],
                }
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
        }
    }

    async fn handle_get_children(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = GetChildrenRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        if req.watch {
            tree.watch_state
                .register(session_id, req.path.to_string(), WatchType::ListWatch);
        }

        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };

                let mut children: Vec<&String> = node.children.iter().collect();
                children.sort();

                let mut response = reply_header.to_bytes();

                let res = GetChildrenResponse {
                    children: &children,
                };
                response.extend(res.to_bytes());
                response.extend(
                    node.stat
                        .to_bytes(node.data.len() as i32, node.children.len() as i32),
                );
                ApplyResult {
                    response,
                    watch_events: vec![],
                }
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
        }
    }

    async fn handle_get_children_simple(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = GetChildrenRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        if req.watch {
            tree.watch_state
                .register(session_id, req.path.to_string(), WatchType::ListWatch);
        }

        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::Ok,
                };

                let mut children: Vec<&String> = node.children.iter().collect();
                children.sort();

                let mut response = reply_header.to_bytes();

                let res = GetChildrenResponse {
                    children: &children,
                };
                response.extend(res.to_bytes());
                ApplyResult {
                    response,
                    watch_events: vec![],
                }
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
        }
    }
    async fn handle_create(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = CreateRequest::from_bytes(buf) else {
            println!("Failed to parse CreateRequest payload!");
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        if tree.exists(req.path) {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::NodeExists,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        }

        let (parent_path, child_name) = match req.path.rsplit_once("/") {
            Some((p, c)) => (p, c),
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::BadArguments,
                };
                return ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                };
            }
        };

        if child_name.is_empty() {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
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
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        }

        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let op = WalOperation::Create {
            path: req.path.to_string(),
            data: req.data.to_vec(),
            timestamp: now,
            flags: req.flags,
            session_id: session_id.0,
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
                err: ErrorCode::SystemError,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        }
        drop(wal);

        let created_path =
            match tree.create(req.path, req.data.to_vec(), now, session_id, req.flags) {
                Ok(p) => p,
                Err(_) => {
                    let reply_header = ReplyHeader {
                        xid: header.xid,
                        zxid: tree.last_zxid(),
                        err: ErrorCode::BadArguments, // pick the right code per Err string later
                    };
                    return ApplyResult {
                        response: reply_header.to_bytes(),
                        watch_events: vec![],
                    };
                }
            };

        let watch_events = tree
            .watch_state
            .fire(&created_path, WatchEventType::Created);

        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid: tree.last_zxid(),
            err: ErrorCode::Ok,
        };
        let create_res = CreateResponse {
            path: &created_path,
        };

        let mut response = reply_header.to_bytes();
        response.extend(create_res.to_bytes());
        ApplyResult {
            response,
            watch_events,
        }
    }

    async fn handle_get(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = GetDataRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        if req.watch {
            tree.watch_state
                .register(session_id, req.path.to_string(), WatchType::Watch);
        }

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
                let mut response = reply_header.to_bytes();
                response.extend(get_res.to_bytes(node.children.len() as i32));
                ApplyResult {
                    response,
                    watch_events: vec![],
                }
            }
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                }
            }
        }
    }

    async fn handle_set(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = SetDataRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        let current_version = match tree.traverse(req.path) {
            Some(node) => node.stat.version,
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                return ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                };
            }
        };

        if req.version != -1 && current_version != req.version {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadVersion,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
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
                err: ErrorCode::SystemError,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        }
        drop(wal);

        let _ = tree.set(req.path, req.data.to_vec(), now);

        let watch_events = tree.watch_state.fire(req.path, WatchEventType::Changed);

        let node = tree.traverse(req.path).unwrap();
        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid: tree.last_zxid(),
            err: ErrorCode::Ok,
        };
        let set_res = SetDataResponse { stat: &node.stat };
        let mut response = reply_header.to_bytes();
        response.extend(set_res.to_bytes(node.data.len() as i32, node.children.len() as i32));
        ApplyResult {
            response,
            watch_events,
        }
    }

    async fn handle_delete(
        &self,
        header: RequestHeader,
        buf: &mut &[u8],
        session_id: SessionId,
    ) -> ApplyResult {
        let Some(req) = DeleteRequest::from_bytes(buf) else {
            let tree = self.storage.read().await;
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadArguments,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        };

        let mut tree = self.storage.write().await;

        let node = match tree.traverse(req.path) {
            Some(node) => node,
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: tree.last_zxid(),
                    err: ErrorCode::NoNode,
                };
                return ApplyResult {
                    response: reply_header.to_bytes(),
                    watch_events: vec![],
                };
            }
        };

        if req.version != -1 && node.stat.version != req.version {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::BadVersion,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        }

        if !node.children.is_empty() {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: tree.last_zxid(),
                err: ErrorCode::NotEmpty,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
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
                err: ErrorCode::SystemError,
            };
            return ApplyResult {
                response: reply_header.to_bytes(),
                watch_events: vec![],
            };
        }
        drop(wal);

        let owner = tree.traverse(req.path).unwrap().stat.ephemeral_owner;
        let _ = tree.delete(req.path);
        if owner != SessionId(0) {
            tree.session_state.remove_ephemeral(owner, req.path);
        }

        let watch_events = tree.watch_state.fire(req.path, WatchEventType::Deleted);

        let reply_header = ReplyHeader {
            xid: header.xid,
            zxid: tree.last_zxid(),
            err: ErrorCode::Ok,
        };
        let mut response = reply_header.to_bytes();
        response.extend(EmptyResponse.to_bytes());
        ApplyResult {
            response,
            watch_events,
        }
    }
}
