use std::sync::Arc;
use std::time::Duration;

use crate::auth::{AuthId, authenticate};
use crate::dispatcher::KeeperDispatcher;
use crate::protocol::{
    AuthRequest, ConnectRequest, ErrorCode, OpCode, ReplyHeader, RequestHeader, SessionId,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn read_frame(
    reader: &mut (impl AsyncReadExt + Unpin),
    timeout: Duration,
) -> Option<Vec<u8>> {
    let mut length_buffer = [0u8; 4];
    match tokio::time::timeout(timeout, reader.read_exact(&mut length_buffer)).await {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => {
            println!("Connection closed by client");
            return None;
        }
        Err(_) => {
            println!("Connection idle timeout, closing");
            return None;
        }
    }

    let message_length = i32::from_be_bytes(length_buffer);
    if !(0..=1_048_575).contains(&message_length) {
        println!("Message too large or invalid, dropping connection!");
        return None;
    }

    let mut payload = vec![0u8; message_length as usize];
    match tokio::time::timeout(timeout, reader.read_exact(&mut payload)).await {
        Ok(Ok(_)) => Some(payload),
        Ok(Err(_)) => {
            println!("Connection closed by client");
            None
        }
        Err(_) => {
            println!("Connection idle timeout, closing");
            None
        }
    }
}

async fn write_frame(writer: &mut (impl AsyncWriteExt + Unpin), data: &[u8]) -> bool {
    let length = data.len() as i32;
    if writer.write_all(&length.to_be_bytes()).await.is_err() {
        return false;
    }
    writer.write_all(data).await.is_ok()
}

pub struct ConnectionHandler {
    socket: TcpStream,
    dispatcher: Arc<KeeperDispatcher>,
    idle_timeout: Duration,
    session_id: Option<SessionId>,
    auth_ids: Vec<AuthId>,
}

impl ConnectionHandler {
    pub fn new(
        socket: TcpStream,
        dispatcher: Arc<KeeperDispatcher>,
        idle_timeout: Duration,
    ) -> Self {
        ConnectionHandler {
            socket,
            dispatcher,
            idle_timeout,
            session_id: None,
            auth_ids: Vec::new(),
        }
    }

    pub async fn run(mut self) {
        let Some(first_frame) = read_frame(&mut self.socket, self.idle_timeout).await else {
            return;
        };

        // Four-letter commands (ruok, stat, conf, envi, mntr, ...) produce
        // frames whose 4-byte length prefix, read as an i32, falls outside
        // the valid range — read_frame already rejects those. If we get here
        // the frame is a valid ConnectRequest.
        let mut buf = first_frame.as_slice();
        let Some(connect_request) = ConnectRequest::from_bytes(&mut buf) else {
            println!("Failed to parse ConnectRequest");
            return;
        };

        let (connect_response, mut watch_rx) = self.dispatcher.handshake(connect_request).await;
        self.session_id = Some(SessionId(connect_response.session_id));

        if !write_frame(&mut self.socket, &connect_response.to_bytes()).await {
            return;
        }

        println!("Handshake complete, session_id: {:?}", self.session_id);

        let session_id = self.session_id.unwrap();
        let dispatcher = Arc::clone(&self.dispatcher);
        let idle_timeout = self.idle_timeout;
        let (mut reader, mut writer) = self.socket.split();

        loop {
            tokio::select! {
                frame = read_frame(&mut reader, idle_timeout) => {
                    let Some(payload) = frame else { break };

                    if payload.len() < 8 {
                        break;
                    }

                    let mut buf = payload.as_slice();
                    let Ok(header) = RequestHeader::from_bytes(&mut buf) else {
                        break;
                    };

                    dispatcher.touch_session(session_id).await;

                    if header.opcode == OpCode::Auth {
                        let Some(request) = AuthRequest::from_bytes(&mut buf) else {
                            break;
                        };

                        if !buf.is_empty() {
                            break;
                        }

                        let err = match authenticate(request.scheme, request.auth) {
                            Ok(identity) => {
                                if !self.auth_ids.contains(&identity) {
                                    self.auth_ids.push(identity);
                                }
                                ErrorCode::Ok
                            }
                            Err(_) => ErrorCode::AuthFailed,
                        };

                        let response = ReplyHeader {
                            xid: header.xid,
                            zxid: 0,
                            err,
                        };

                        if !write_frame(&mut writer, &response.to_bytes()).await {
                            break;
                        }

                        if err != ErrorCode::Ok {
                            break;
                        }

                        continue;
                    }



                    let response = dispatcher.dispatch(payload, session_id).await;

                    if !response.is_empty() && !write_frame(&mut writer, &response).await {
                        break;
                    }
                    if header.opcode == OpCode::Close {
                        println!("Close received, shutting down connection");
                        break;
                    }
                }
                Some(notification) = watch_rx.recv() => {
                    if !write_frame(&mut writer, &notification).await {
                        break;
                    }
                }
            }
        }

        dispatcher.remove_watch_sender(session_id).await;
    }
}
