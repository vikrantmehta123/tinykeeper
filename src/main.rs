mod protocol;
mod storage;
mod wal;
mod znode;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

use crate::protocol::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:2181").await?;
    println!("tinykeeper is running on 127.0.0.1:2181");

    let global_storage = Arc::new(RwLock::new(storage::KeeperStorage::new()));

    // WAL Replay Phase
    {
        let mut tree = global_storage.write().await;
        wal::WalManager::replay("tinykeeper.wal", &mut tree).await;
    }

    let global_wal = Arc::new(Mutex::new(wal::WalManager::new("tinykeeper.wal").await?));

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("New client connected from: {}", addr);

        let client_storage = Arc::clone(&global_storage);
        let client_wal = Arc::clone(&global_wal);

        tokio::spawn(async move {
            let mut length_buffer = [0u8; 4];

            if socket.read_exact(&mut length_buffer).await.is_ok() {
                let message_length = i32::from_be_bytes(length_buffer);

                // Production-grade safeguard!
                if !(0..=1_048_575).contains(&message_length) {
                    println!("Message too large or invalid, dropping connection!");
                    return; // Exit this client's async task to drop the connection
                }

                // Create a buffer to hold the payload
                let mut payload_buffer = vec![0u8; message_length as usize];

                if socket.read_exact(&mut payload_buffer).await.is_ok() {
                    let mut buf = payload_buffer.as_slice();

                    if message_length >= 8 {
                        let header = protocol::RequestHeader::from_bytes(&mut buf);
                        println!(
                            "Parsed Header -> xid: {}, OpCode: {}",
                            header.xid, header.opcode
                        );

                        match header.opcode {
                            1 => {
                                if let Some(req) = protocol::CreateRequest::from_bytes(&mut buf) {
                                    // Brute Force: Get the tree lock early.
                                    // TODO: In future, this should be pipelined
                                    let mut tree = client_storage.write().await;

                                    if tree.exists(req.path) {
                                        let reply_header = ReplyHeader {
                                            xid: header.xid,
                                            zxid: 0,
                                            err: -110, // ZooKeeper's official NodeExists error code
                                        };
                                        let payload = reply_header.to_bytes();
                                        let len = payload.len() as i32;
                                        let _ = socket.write_all(&len.to_be_bytes()).await;
                                        let _ = socket.write_all(&payload).await;
                                        return; // Exit early! Do NOT write to WAL and do NOT mutate the tree.
                                    }

                                    let mut wal = client_wal.lock().await;
                                    let log_entry = wal::LogRecord::Create {
                                        path: req.path.to_string(),
                                        data: req.data.to_vec(),
                                    };
                                    if let Err(e) = wal.append(&log_entry).await {
                                        println!("Failed to write to WAL: {}", e);
                                        return;
                                    }
                                    
                                    // Dropping the WAL doesn't do anything for us as yet. But
                                    // dropping it because it may be good practice to do so.
                                    drop(wal);

                                    match tree.create(req.path, req.data.to_vec()) {
                                        Ok(_) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0, // temporary placeholder
                                                err: 0,  // no error
                                            };

                                            let create_res = CreateResponse { path: req.path };
                                            let mut response_payload = reply_header.to_bytes();
                                            response_payload.extend(create_res.to_bytes());

                                            let total_length = response_payload.len() as i32;
                                            let _ =
                                                socket.write_all(&total_length.to_be_bytes()).await;
                                            let _ = socket.write_all(&response_payload).await;
                                        }
                                        Err(e) => println!("Failed to create: {}", e),
                                    }
                                } else {
                                    println!("Failed to parse CreateRequest payload!");
                                }
                            }
                            4 => {
                                if let Some(req) = protocol::GetDataRequest::from_bytes(&mut buf) {
                                    let tree = client_storage.read().await;
                                    match tree.get(req.path) {
                                        Ok(data) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0,
                                                err: 0,
                                            };
                                            let get_res = GetDataResponse { data };
                                            let mut payload = reply_header.to_bytes();
                                            payload.extend(get_res.to_bytes());
                                            let len = payload.len() as i32;
                                            let _ = socket.write_all(&len.to_be_bytes()).await;
                                            let _ = socket.write_all(&payload).await;
                                        }
                                        Err(_) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0,
                                                err: -101,
                                            }; // NoNode
                                            let payload = reply_header.to_bytes();
                                            let len = payload.len() as i32;
                                            let _ = socket.write_all(&len.to_be_bytes()).await;
                                            let _ = socket.write_all(&payload).await;
                                        }
                                    }
                                }
                            }
                            5 => {
                                if let Some(req) = protocol::SetDataRequest::from_bytes(&mut buf) {
                                    // Brute Force: Get the tree lock early.
                                    // TODO: In future, this should be pipelined
                                    let mut tree = client_storage.write().await;

                                    if !tree.exists(req.path) {
                                        let reply_header = ReplyHeader {
                                            xid: header.xid,
                                            zxid: 0,
                                            err: -101, // NoNode
                                        };
                                        let payload = reply_header.to_bytes();
                                        let len = payload.len() as i32;
                                        let _ = socket.write_all(&len.to_be_bytes()).await;
                                        let _ = socket.write_all(&payload).await;
                                        return;
                                    }

                                    let mut wal = client_wal.lock().await;
                                    let log_entry = wal::LogRecord::Set {
                                        path: req.path.to_string(),
                                        data: req.data.to_vec(),
                                    };
                                    if let Err(e) = wal.append(&log_entry).await {
                                        println!("Failed to write to WAL: {}", e);
                                        return;
                                    }
                                    drop(wal);
                                    match tree.set(req.path, req.data.to_vec()) {
                                        Ok(_) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0,
                                                err: 0,
                                            };
                                            let mut payload = reply_header.to_bytes();
                                            payload.extend(EmptyResponse.to_bytes());
                                            let len = payload.len() as i32;
                                            let _ = socket.write_all(&len.to_be_bytes()).await;
                                            let _ = socket.write_all(&payload).await;
                                        }
                                        Err(_) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0,
                                                err: -101,
                                            };
                                            let payload = reply_header.to_bytes();
                                            let len = payload.len() as i32;
                                            let _ = socket.write_all(&len.to_be_bytes()).await;
                                            let _ = socket.write_all(&payload).await;
                                        }
                                    }
                                }
                            }
                            2 => {
                                if let Some(req) = protocol::DeleteRequest::from_bytes(&mut buf) {
                                    // Brute Force: Get the tree lock early.
                                    // TODO: In future, this should be pipelined
                                    let mut tree = client_storage.write().await;

                                    if !tree.exists(req.path) {
                                        let reply_header = ReplyHeader {
                                            xid: header.xid,
                                            zxid: 0,
                                            err: -101, // NoNode
                                        };
                                        let payload = reply_header.to_bytes();
                                        let len = payload.len() as i32;
                                        let _ = socket.write_all(&len.to_be_bytes()).await;
                                        let _ = socket.write_all(&payload).await;
                                        return;
                                    }

                                    let mut wal = client_wal.lock().await;
                                    let log_entry = wal::LogRecord::Delete {
                                        path: req.path.to_string(),
                                    };
                                    if let Err(e) = wal.append(&log_entry).await {
                                        println!("Failed to write to WAL: {}", e);
                                        return;
                                    }
                                    drop(wal);
                                    match tree.delete(req.path) {
                                        Ok(_) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0,
                                                err: 0,
                                            };
                                            let mut payload = reply_header.to_bytes();
                                            payload.extend(EmptyResponse.to_bytes());
                                            let len = payload.len() as i32;
                                            let _ = socket.write_all(&len.to_be_bytes()).await;
                                            let _ = socket.write_all(&payload).await;
                                        }
                                        Err(_) => {
                                            let reply_header = ReplyHeader {
                                                xid: header.xid,
                                                zxid: 0,
                                                err: -101,
                                            };
                                            let payload = reply_header.to_bytes();
                                            let len = payload.len() as i32;
                                            let _ = socket.write_all(&len.to_be_bytes()).await;
                                            let _ = socket.write_all(&payload).await;
                                        }
                                    }
                                }
                            }
                            11 => {
                                // Ping response is just the header
                                let reply_header = ReplyHeader {
                                    xid: header.xid,
                                    zxid: 0,
                                    err: 0,
                                };
                                let payload = reply_header.to_bytes();
                                let len = payload.len() as i32;
                                let _ = socket.write_all(&len.to_be_bytes()).await;
                                let _ = socket.write_all(&payload).await;
                            }
                            _ => println!("Received unknown OpCode: {}", header.opcode),
                        }
                    } else {
                        println!("Message too short to be a standard request");
                    }
                }
            }
        });
    }
}
