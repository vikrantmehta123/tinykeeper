mod storage;
mod znode;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use bytes::Buf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:2181").await?;
    println!("tinyKeeper is running on 127.0.0.1:2181");

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
                        let xid  = buf.get_i32();
                        let opcode = buf.get_i32();

                        println!("Parsed Header -> xid: {}, OpCode: {}", xid, opcode);

                        match opcode {
                            1 => println!("Client wants to CREATE a node!"),
                            2 => println!("Client wants to DELETE a node!"),
                            4 => println!("Client wants to GET data!"),
                            5 => println!("Client wants to SET data!"),
                            11 => println!("Client sent a PING!"),
                            _ => println!("Received unknown OpCode: {}", opcode),
                        }
                    }
                    else {
                        println!("Message too short to be a standard request");
                    }
                }
            }
        });
    }
}
