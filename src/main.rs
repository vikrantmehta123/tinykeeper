mod config;
mod context;
mod dispatcher;
mod keeper_server;
mod protocol;
mod server_uuid;
mod storage;
mod wal;
mod znode;

use std::sync::{Arc, OnceLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::context::KeeperContext;
use crate::dispatcher::KeeperDispatcher;
use crate::keeper_server::KeeperServer;
use crate::server_uuid::ServerUUID;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load config.
    let config = Config::load("keeper_config.toml");

    // 2. Create the storage directory.
    std::fs::create_dir_all(&config.storage_path)?;

    // 3. Load or generate the server UUID.
    let uuid = ServerUUID::load_or_create(&config.storage_path);

    // 4. Build the shared context; dispatcher is filled in once built below.
    let context = Arc::new(KeeperContext {
        config: config.clone(),
        uuid,
        dispatcher: OnceLock::new(),
    });

    // 5. Build KeeperServer (opens/replays the WAL) and KeeperDispatcher.
    let wal_path = config.storage_path.join("tinykeeper.wal");
    let server = Arc::new(KeeperServer::new(wal_path.to_str().unwrap()).await?);
    let dispatcher = Arc::new(KeeperDispatcher::new(server));
    context
        .dispatcher
        .set(Arc::clone(&dispatcher))
        .unwrap_or_else(|_| panic!("dispatcher already set"));

    // NOTE: A config reloader and cgroups observer exist in ClickHouse
    // Keeper's main() but are intentionally dropped here, not stubbed —
    // out of scope for tinyKeeper's current milestone.

    // 6. Bind the single TCP port.
    let listener = TcpListener::bind((config.listen_host.as_str(), config.tcp_port)).await?;
    println!(
        "tinykeeper is running on {}:{}",
        config.listen_host, config.tcp_port
    );

    // 7. Accept loop.
    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("New client connected from: {}", addr);

        let client_dispatcher = Arc::clone(&dispatcher);

        tokio::spawn(async move {
            let mut length_buffer = [0u8; 4];

            if socket.read_exact(&mut length_buffer).await.is_ok() {
                let message_length = i32::from_be_bytes(length_buffer);

                if !(0..=1_048_575).contains(&message_length) {
                    println!("Message too large or invalid, dropping connection!");
                    return;
                }

                let mut payload_buffer = vec![0u8; message_length as usize];

                if socket.read_exact(&mut payload_buffer).await.is_ok() {
                    let response_payload = client_dispatcher.dispatch(payload_buffer).await;

                    if !response_payload.is_empty() {
                        let total_length = response_payload.len() as i32;
                        let _ = socket.write_all(&total_length.to_be_bytes()).await;
                        let _ = socket.write_all(&response_payload).await;
                    }
                }
            }
        });
    }
}
