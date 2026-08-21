mod config;
mod keeper_server;
mod protocol;
mod server_uuid;
mod storage;
mod wal;
mod znode;
mod dispatcher;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:2181").await?;
    println!("tinykeeper is running on 127.0.0.1:2181");

    // TODO(Task 6): construct KeeperServer (owns storage + WAL) and
    // KeeperDispatcher here, then route requests through the dispatcher
    // instead of discarding header/buf below.

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("New client connected from: {}", addr);

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

                        // TODO(Task 6): route through KeeperDispatcher/KeeperServer
                        // and write the framed response back to the socket.
                        let _ = (header, buf);
                    } else {
                        println!("Message too short to be a standard request");
                    }
                }
            }
        });
    }
}
