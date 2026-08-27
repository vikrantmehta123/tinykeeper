use tokio::sync::{Mutex, RwLock};

use serde::{Serialize, Deserialize};
use crate::protocol::*;
use crate::storage::KeeperStorage;
use crate::changelog::WalStore;
use std::path::Path;

#[derive(Serialize, Deserialize)]
enum WalOperation {
    Create { path: String, data: Vec<u8> },
    Set { path: String, data: Vec<u8> },
    Delete { path: String },
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
        let mut storage = KeeperStorage::new();
    
        let wal = WalStore::open(wal_dir, 50 * 1024 * 1024).await?;
    
        wal.replay(|_index, _term, payload| {
            if let Some(op) = WalOperation::deserialize(&payload) {
                match op {
                    WalOperation::Create { path, data } => { let _ = storage.create(&path, data); }
                    WalOperation::Set { path, data } => { let _ = storage.set(&path, data); }
                    WalOperation::Delete { path } => { let _ = storage.delete(&path); }
                }
            }
        }).await?;
    
        Ok(KeeperServer {
            storage: RwLock::new(storage),
            wal: Mutex::new(wal),
        })
    }

    pub async fn apply(&self, payload: &[u8]) -> Vec<u8> {
        if payload.len() < 8 {
            println!("Message too short to be a standard request");
            return Vec::new();
        }

        let mut buf = payload;
        let header = RequestHeader::from_bytes(&mut buf).expect("unknown opcode");
        println!(
            "Parsed Header -> xid: {}, OpCode: {:?}",
            header.xid, header.opcode
        );
        match header.opcode {
            OpCode::Create => self.handle_create(header, &mut buf).await,
            OpCode::Get => self.handle_get(header, &mut buf).await,
            OpCode::Set => self.handle_set(header, &mut buf).await,
            OpCode::Remove => self.handle_delete(header, &mut buf).await,
            OpCode::Heartbeat => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::Ok,
                };
                reply_header.to_bytes()
            }
            OpCode::List => self.handle_get_children(header, &mut buf).await,
            OpCode::Exists => self.handle_exists(header, &mut buf).await,
            _ => {
                println!("Received unimplemented OpCode: {:?}", header.opcode);
                Vec::new()
            }
        }
    }
    async fn handle_exists(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = ExistsRequest::from_bytes(buf) else {
            return Vec::new();
        };

        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
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
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_get_children(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = GetChildrenRequest::from_bytes(buf) else {
            return Vec::new();
        };
        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::Ok,
                };
                let mut children: Vec<&String> = node.children.keys().collect();
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
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_create(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = CreateRequest::from_bytes(buf) else {
            println!("Failed to parse CreateRequest payload!");
            return Vec::new();
        };

        // Brute Force: Get the tree lock early.
        // TODO: In future, this should be pipelined
        let mut tree = self.storage.write().await;

        if tree.exists(req.path) {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: 0,
                err: ErrorCode::NodeExists,
            };
            return reply_header.to_bytes(); // Exit early! Do NOT write to WAL and do NOT mutate the tree.
        }

        let op = WalOperation::Create {
            path: req.path.to_string(),
            data: req.data.to_vec(),
        };
        let payload = op.serialize();

        let mut wal = self.wal.lock().await;
        wal.append(1, 0, 0, 0, &payload);
        if let Err(e) = wal.flush().await {
            println!("Failed to write to WAL: {}", e);
            return Vec::new();
        }
        drop(wal);

        match tree.create(req.path, req.data.to_vec()) {
            Ok(_) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0, // temporary placeholder
                    err: ErrorCode::Ok,
                };

                let create_res = CreateResponse { path: req.path };
                let mut response_payload = reply_header.to_bytes();
                response_payload.extend(create_res.to_bytes());
                response_payload
            }
            Err(e) => {
                println!("Failed to create: {}", e);
                Vec::new()
            }
        }
    }

    async fn handle_get(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = GetDataRequest::from_bytes(buf) else {
            return Vec::new();
        };

        let tree = self.storage.read().await;
        match tree.traverse(req.path) {
            Some(node) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
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
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_set(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = SetDataRequest::from_bytes(buf) else {
            return Vec::new();
        };

        // Brute Force: Get the tree lock early.
        // TODO: In future, this should be pipelined
        let mut tree = self.storage.write().await;

        let current_version = match tree.traverse(req.path) {
            Some(node) => node.stat.version,
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                return reply_header.to_bytes();
            }
        };

        if req.version != -1 && current_version != req.version {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: 0,
                err: ErrorCode::BadVersion,
            };
            return reply_header.to_bytes();
        }

        let op = WalOperation::Set {
            path: req.path.to_string(),
            data: req.data.to_vec(),
        };
        let payload = op.serialize();
        
        let mut wal = self.wal.lock().await;
        wal.append(1, 0, 0, 0, &payload);
        if let Err(e) = wal.flush().await {
            println!("Failed to write to WAL: {}", e);
            return Vec::new();
        }
        drop(wal);

        match tree.set(req.path, req.data.to_vec()) {
            Ok(_) => {
                let node = tree.traverse(req.path).unwrap();
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::Ok,
                };
                let set_res = SetDataResponse { stat: &node.stat };
                let mut payload = reply_header.to_bytes();
                payload
                    .extend(set_res.to_bytes(node.data.len() as i32, node.children.len() as i32));
                payload
            }
            Err(_) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }

    async fn handle_delete(&self, header: RequestHeader, buf: &mut &[u8]) -> Vec<u8> {
        let Some(req) = DeleteRequest::from_bytes(buf) else {
            return Vec::new();
        };

        // Brute Force: Get the tree lock early.
        // TODO: In future, this should be pipelined
        let mut tree = self.storage.write().await;

        let current_version = match tree.traverse(req.path) {
            Some(node) => node.stat.version,
            None => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                return reply_header.to_bytes();
            }
        };

        if req.version != -1 && current_version != req.version {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: 0,
                err: ErrorCode::BadVersion,
            };
            return reply_header.to_bytes();
        }

        let op = WalOperation::Delete {
            path: req.path.to_string(),
        };
        let payload = op.serialize();
        
        let mut wal = self.wal.lock().await;
        wal.append(1, 0, 0, 0, &payload);
        if let Err(e) = wal.flush().await {
            println!("Failed to write to WAL: {}", e);
            return Vec::new();
        }
        drop(wal);

        match tree.delete(req.path) {
            Ok(_) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::Ok,
                };
                let mut payload = reply_header.to_bytes();
                payload.extend(EmptyResponse.to_bytes());
                payload
            }
            Err(_) => {
                let reply_header = ReplyHeader {
                    xid: header.xid,
                    zxid: 0,
                    err: ErrorCode::NoNode,
                };
                reply_header.to_bytes()
            }
        }
    }
}
