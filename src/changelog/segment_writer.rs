//! SegmentWriter
//!
//! A segment is one WAL file. Instead of one ever-growing file,
//! the WAL is split into multiple files. Each one is a segment.
//!
//! Each segment covers a range of log indices. Typically, we define
//! a naming scheme such that the file name itself tells you the range
//! of log indices that the file contains.
//!
//! SegmentWriter cares about the one active segment, the one to which
//! the WAL record is to be written.
//!
//! SegmentWriter knows three things:
//! * A file handle — the one open segment it's writing to
//! * A buffer — where encode puts bytes before they hit disk
//! * How to flush — write the buffer to the file, fsync
//!
//! It doesn't know how to interpret the byte buffer, segment file rotation,
//! compaction, or even file names.

use bytes::BytesMut;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use super::WalError;
use super::record;

pub(crate) struct SegmentWriter {
    file: File,
    buf: BytesMut, // A buffer to store wal records. We can store multiple records here.
    bytes_written: u64, // Track how much data is there on the file + in buffer. Helps in rotation
}

impl SegmentWriter {
    pub fn new(file: File) -> Self {
        SegmentWriter {
            file,
            buf: BytesMut::new(),
            bytes_written: 0u64,
        }
    }

    pub fn with_existing(file: File, existing_bytes: u64) -> Self {
        SegmentWriter {
            file,
            buf: BytesMut::new(),
            bytes_written: existing_bytes,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Append one wal record to the buffer but don't flush it to disk yet
    pub fn append(&mut self, version: u8, index: u64, term: u64, value_type: u8, payload: &[u8]) {
        let before = self.buf.len();
        record::encode(&mut self.buf, version, index, term, value_type, payload);
        self.bytes_written += (self.buf.len() - before) as u64;
    }

    /// Flush all the wal records in the buffer on to disk
    pub async fn flush(&mut self) -> Result<(), WalError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buf).await?;
        self.file.sync_data().await?;
        self.buf.clear();
        Ok(())
    }
}
