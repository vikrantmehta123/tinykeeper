use std::sync::Arc;

use crate::keeper_server::KeeperServer;
use crate::protocol::SessionId;
use crate::protocol::{ConnectRequest, ConnectResponse};

pub struct KeeperDispatcher {
    server: Arc<KeeperServer>,
}

impl KeeperDispatcher {
    pub fn new(server: Arc<KeeperServer>) -> Self {
        KeeperDispatcher { server }
    }

    pub async fn dispatch(&self, payload: Vec<u8>, session_id: SessionId) -> Vec<u8> {
        let server = Arc::clone(&self.server);

        let handle = tokio::spawn(async move { server.apply(&payload, session_id).await });

        handle.await.unwrap_or_default()
    }

    pub async fn handshake(&self, request: ConnectRequest) -> ConnectResponse {
        let session_id = self.server.create_session(request.timeout_ms as i64).await;

        ConnectResponse {
            protocol_version: request.protocol_version,
            timeout_ms: request.timeout_ms,
            session_id: session_id.0,
            password: Vec::new(),
        }
    }

    pub fn shutdown(&self) {
        // Stub: no background resources to stop yet with the tokio::spawn
        // approach. Will matter once session tracking has state to flush.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatch_reaches_keeper_server_and_returns_response() {
        let dir =
            std::env::temp_dir().join(format!("tinykeeper-dispatch-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("test.wal");

        let server = Arc::new(KeeperServer::new(&wal_path).await.unwrap());
        let dispatcher = KeeperDispatcher::new(server);

        // xid=42 (4 bytes), opcode=11/ping (4 bytes), no body needed.
        let mut payload = Vec::new();
        payload.extend_from_slice(&42i32.to_be_bytes());
        payload.extend_from_slice(&11i32.to_be_bytes());

        let response = dispatcher.dispatch(payload, SessionId(1)).await;

        // ReplyHeader is xid(4) + zxid(8) + err(4) = 16 bytes.
        assert_eq!(response.len(), 16);
        assert_eq!(&response[0..4], &42i32.to_be_bytes());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
