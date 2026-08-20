use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::storage::KeeperStorage;

// Serde magically generates the byte serialization code for this!
#[derive(Serialize, Deserialize, Debug)]
pub enum LogRecord {
    Create { path: String, data: Vec<u8> },
    Set { path: String, data: Vec<u8> },
    Delete { path: String },
}

pub struct WalManager {
    file: File,
}

impl WalManager {
    pub async fn new(filename: &str) -> Result<Self, std::io::Error> {
        // Open the file in append mode. Create it if it doesn't exist.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)
            .await?;

        Ok(Self { file })
    }

    pub async fn append(&mut self, record: &LogRecord) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Serialize the Rust enum into a raw Vec<u8> using postcard
        let bytes = postcard::to_allocvec(record)?;

        // 2. Write the length of the payload first (so we can read it back easily on reboot!)
        let len = bytes.len() as u32;
        self.file.write_all(&len.to_be_bytes()).await?;

        // 3. Write the actual serialized log entry
        self.file.write_all(&bytes).await?;

        // 4. THE MOST IMPORTANT LINE: Fsync!
        // This forces the OS to physically write the data to the hardware SSD/Disk before returning
        self.file.sync_data().await?;

        Ok(())
    }

    pub async fn replay(filename: &str, tree: &mut KeeperStorage) {
        if let Ok(mut f) = tokio::fs::File::open(filename).await {
            println!("Found WAL file, starting replay...");
            let mut count = 0;
            loop {
                let mut len_buf = [0u8; 4];
                if f.read_exact(&mut len_buf).await.is_err() {
                    break; // EOF or partial read
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut record_buf = vec![0u8; len];
                if f.read_exact(&mut record_buf).await.is_err() {
                    println!("Warning: Partial WAL record detected at EOF.");
                    break;
                }
                if let Ok(record) = postcard::from_bytes::<LogRecord>(&record_buf) {
                    match record {
                        LogRecord::Create { path, data } => {
                            let _ = tree.create(&path, data);
                        }
                        LogRecord::Set { path, data } => {
                            let _ = tree.set(&path, data);
                        }
                        LogRecord::Delete { path } => {
                            let _ = tree.delete(&path);
                        }
                    }
                    count += 1;
                } else {
                    println!("Warning: Failed to deserialize WAL record.");
                    break;
                }
            }
            println!("Replayed {} transactions from WAL.", count);
        }
    }
}
