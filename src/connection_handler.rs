use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::dispatcher::KeeperDispatcher;
use crate::protocol::{ConnectRequest, ConnectResponse};

pub struct ConnectionHandler {
    socket: TcpStream,
    dispatcher: Arc<KeeperDispatcher>,
    idle_timeout: Duration,
    session_id: Option<i64>,
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
        }
    }

    pub async fn run(mut self) {
        let mut first_bytes = [0u8; 4];
        match tokio::time::timeout(self.idle_timeout, self.socket.read_exact(&mut first_bytes))
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
        let connect_request_length = i32::from_be_bytes(first_bytes);

        if !(0..=1_048_575).contains(&connect_request_length) {
            // Four-letter commands (ruok, stat, conf, envi, mntr, ...), read
            // as a big-endian i32, land way outside a valid frame length —
            // that's how we distinguish them from a real ConnectRequest here.
            println!(
                "Received likely four-letter command (raw bytes: {:?})",
                first_bytes
            );
            // Out of scope for v1 — deliberately unimplemented.
            todo!("four-letter commands are out of scope for v1");
        }

        let mut connect_request_buffer = vec![0u8; connect_request_length as usize];
        match tokio::time::timeout(
            self.idle_timeout,
            self.socket.read_exact(&mut connect_request_buffer),
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

        let mut buf = connect_request_buffer.as_slice();
        let Some(connect_request) = ConnectRequest::from_bytes(&mut buf) else {
            println!("Failed to parse ConnectRequest");
            return;
        };

        let connect_response: ConnectResponse = self.dispatcher.handshake(connect_request);
        self.session_id = Some(connect_response.session_id);

        let response_bytes = connect_response.to_bytes();
        let total_length = response_bytes.len() as i32;
        if self
            .socket
            .write_all(&total_length.to_be_bytes())
            .await
            .is_err()
        {
            return;
        }
        if self.socket.write_all(&response_bytes).await.is_err() {
            return;
        }

        println!("Handshake complete, session_id: {:?}", self.session_id);

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
