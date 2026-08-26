use tokio::sync::{Mutex, RwLock};

use crate::protocol::*;
use crate::storage::KeeperStorage;
use crate::wal::{self, WalManager};

pub struct KeeperServer {
    storage: RwLock<KeeperStorage>,
    wal: Mutex<WalManager>,
}

impl KeeperServer {
    pub async fn new(wal_path: &str) -> std::io::Result<Self> {
        let mut storage = KeeperStorage::new();
        WalManager::replay(wal_path, &mut storage).await;
        let wal = WalManager::new(wal_path).await?;

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
            _ => {
                println!("Received unimplemented OpCode: {:?}", header.opcode);
                Vec::new()
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

        let mut wal = self.wal.lock().await;
        let log_entry = wal::LogRecord::Create {
            path: req.path.to_string(),
            data: req.data.to_vec(),
        };
        if let Err(e) = wal.append(&log_entry).await {
            println!("Failed to write to WAL: {}", e);
            return Vec::new();
        }

        // Dropping the WAL doesn't do anything for us as yet. But
        // dropping it because it may be good practice to do so.
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

        if !tree.exists(req.path) {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: 0,
                err: ErrorCode::NoNode,
            };
            return reply_header.to_bytes();
        }

        let mut wal = self.wal.lock().await;
        let log_entry = wal::LogRecord::Set {
            path: req.path.to_string(),
            data: req.data.to_vec(),
        };
        if let Err(e) = wal.append(&log_entry).await {
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

        if !tree.exists(req.path) {
            let reply_header = ReplyHeader {
                xid: header.xid,
                zxid: 0,
                err: ErrorCode::NoNode,
            };
            return reply_header.to_bytes();
        }

        let mut wal = self.wal.lock().await;
        let log_entry = wal::LogRecord::Delete {
            path: req.path.to_string(),
        };
        if let Err(e) = wal.append(&log_entry).await {
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
