use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::keeper_server::KeeperServer;
use crate::protocol::RequestHeader;

pub struct KeeperDispatcher {
    server: Arc<KeeperServer>,
    // Session routing: structurally present, not implemented yet.
    _sessions: Mutex<HashMap<i64, ()>>,
}

impl KeeperDispatcher {
    pub fn new(server: Arc<KeeperServer>) -> Self {
        KeeperDispatcher {
            server,
            _sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn dispatch(&self, header: RequestHeader, payload: Vec<u8>) -> Vec<u8> {
        let server = Arc::clone(&self.server);

        let handle = tokio::spawn(async move {
            let mut buf = payload.as_slice();
            server.apply(header, &mut buf).await
        });

        handle.await.unwrap_or_default()
    }

    pub fn shutdown(&self) {
        // Stub: no background resources to stop yet with the tokio::spawn
        // approach. Will matter once session tracking has state to flush.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::RequestHeader;

    #[tokio::test]
    async fn dispatch_reaches_keeper_server_and_returns_response() {
        let dir = std::env::temp_dir().join(format!(
            "tinykeeper-dispatch-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("test.wal");

        let server = Arc::new(
            KeeperServer::new(wal_path.to_str().unwrap()).await.unwrap(),
        );
        let dispatcher = KeeperDispatcher::new(server);

        // opcode 11 = ping, no payload needed.
        let header = RequestHeader { xid: 42, opcode: 11 };
        let response = dispatcher.dispatch(header, Vec::new()).await;

        // ReplyHeader is xid(4) + zxid(8) + err(4) = 16 bytes.
        assert_eq!(response.len(), 16);
        assert_eq!(&response[0..4], &42i32.to_be_bytes());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
