mod changelog;
mod config;
mod connection_handler;
mod context;
mod dispatcher;
mod keeper_server;
mod protocol;
mod server_uuid;
mod storage;
mod znode;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::net::TcpListener;

use crate::config::Config;
use crate::connection_handler::ConnectionHandler;
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
    let wal_dir = config.storage_path.join("changelog");
    let server = Arc::new(KeeperServer::new(&wal_dir).await?);
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

    let idle_timeout = Duration::from_secs(config.idle_timeout_secs);

    // 7. Accept loop.
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client connected from: {}", addr);

        let handler = ConnectionHandler::new(socket, Arc::clone(&dispatcher), idle_timeout);
        tokio::spawn(handler.run());
    }
}
