use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::dispatcher::KeeperDispatcher;

pub struct ConnectionHandler {
    socket: TcpStream,
    dispatcher: Arc<KeeperDispatcher>,
    idle_timeout: Duration,
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
        }
    }

    pub async fn run(mut self) {
        loop {
            let mut length_buffer = [0u8; 4];

            match tokio::time::timeout(
                self.idle_timeout,
                self.socket.read_exact(&mut length_buffer),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {
                    println!("Connection closed by client");
                    return;
                }
                Err(_) => {
                    println!("Connection idle timeout, closing");
                    return;
                }
            }

            let message_length = i32::from_be_bytes(length_buffer);

            if !(0..=1_048_575).contains(&message_length) {
                println!("Message too large or invalid, dropping connection!");
                return;
            }

            let mut payload_buffer = vec![0u8; message_length as usize];

            match tokio::time::timeout(
                self.idle_timeout,
                self.socket.read_exact(&mut payload_buffer),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {
                    println!("Connection closed by client");
                    return;
                }
                Err(_) => {
                    println!("Connection idle timeout, closing");
                    return;
                }
            }

            let response_payload = self.dispatcher.dispatch(payload_buffer).await;

            if !response_payload.is_empty() {
                let total_length = response_payload.len() as i32;
                if self
                    .socket
                    .write_all(&total_length.to_be_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
                if self.socket.write_all(&response_payload).await.is_err() {
                    return;
                }
            }
        }
    }
}
