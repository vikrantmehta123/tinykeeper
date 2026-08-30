use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};

use crate::keeper_server::KeeperServer;
use crate::protocol::SessionId;
use crate::protocol::{ConnectRequest, ConnectResponse};
use crate::watch_state::ApplyResult;

pub struct KeeperDispatcher {
    server: Arc<KeeperServer>,
    watch_senders: RwLock<HashMap<SessionId, mpsc::Sender<Vec<u8>>>>,
}

impl KeeperDispatcher {
    pub fn new(server: Arc<KeeperServer>, check_interval_ms: u64) -> Self {
        let bg_server = Arc::clone(&server);
        tokio::spawn(async move {
            let interval = Duration::from_millis(check_interval_ms);
            loop {
                tokio::time::sleep(interval).await;
                let expired = bg_server.get_expired_sessions().await;
                for id in expired {
                    bg_server.close_session(id).await;
                }
            }
        });

        KeeperDispatcher {
            server,
            watch_senders: RwLock::new(HashMap::new()),
        }
    }

    pub async fn dispatch(&self, payload: Vec<u8>, session_id: SessionId) -> Vec<u8> {
        let server = Arc::clone(&self.server);

        let handle = tokio::spawn(async move { server.apply(&payload, session_id).await });

        let result = handle.await.unwrap_or_else(|_| ApplyResult {
            response: vec![],
            watch_events: vec![],
        });

        let senders = self.watch_senders.read().await;
        for event in result.watch_events {
            if let Some(tx) = senders.get(&event.session_id) {
                let _ = tx.send(event.payload).await;
            }
        }

        result.response
    }

    pub async fn handshake(&self, request: ConnectRequest) -> (ConnectResponse, mpsc::Receiver<Vec<u8>>) {
        let session_id = self.server.create_session(request.timeout_ms as i64).await;

        let (tx, rx) = mpsc::channel(64);
        self.watch_senders.write().await.insert(SessionId(session_id.0), tx);

        let response = ConnectResponse {
            protocol_version: request.protocol_version,
            timeout_ms: request.timeout_ms,
            session_id: session_id.0,
            password: Vec::new(),
        };

        (response, rx)
    }

    pub async fn remove_watch_sender(&self, session_id: SessionId) {
        self.watch_senders.write().await.remove(&session_id);
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
        let dispatcher = KeeperDispatcher::new(server, 500);

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
