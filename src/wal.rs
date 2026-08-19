use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

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
}

