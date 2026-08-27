//! WalStore
//!
//! WalStore is the top-level piece in the changelog. It owns the knowledge of big picture:
//! * The directory where segment files live
//! * A map of which segment starts at which index
//! * The active SegmentWriter for the current segment
//! * Rotation — when the active segment is big enough, close it, open a new one
//! * The read path — on recovery, scan segments, decode records in order
//! * Compaction — delete old segments that are no longer needed
//!
//! It's the only thing the rest of your application talks to. SegmentWriter and record are invisible outside the changelog module.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};

use super::segment_writer::SegmentWriter;
use super::record;
use super::WalError;

pub struct WalStore {
    dir: PathBuf,
    segments: BTreeMap<u64, PathBuf>, // Older segments. u64 key corresponds to the start range of
                                      // Log Index. Using this start_index, we can identify
                                      // the segment file's path
    writer: SegmentWriter, // active segment writer
    max_segment_bytes: u64, // For segment file rotation, what's the maximum size of segment
    
    last_index: u64,
}

impl WalStore {
    /// open() needs to do three things:
    /// 1. Scan the directory — find all existing segment files, parse their names to get the first_index, build the BTreeMap
    /// 2. Open the latest segment for writing — that becomes the SegmentWriter
    /// 3. Handle the fresh start case — no segments exist yet, create the first one
    pub async fn open(dir: &Path, max_segment_bytes: u64) -> Result<Self, WalError> {
        fs::create_dir_all(dir).await?;
    
        let mut segments = BTreeMap::new();
        let mut entries = fs::read_dir(dir).await?;
    
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(index) = parse_segment_name(&name) {
                segments.insert(index, entry.path());
            }
        }

        // We need to find the highest index in the last segment. 
        // That means reading the last segment and decoding records to find it.
        let mut last_index = 0u64;
        if let Some((_first_index, path)) = segments.last_key_value() {
            let data = fs::read(path).await?;
            let mut buf: &[u8] = &data;
            while let Some((index, _term, _payload)) = record::decode(&mut buf)? {
                last_index = index;
            }
        }

        let writer = if let Some((_index, path)) = segments.last_key_value() {
            let metadata = fs::metadata(path).await?;
            let file = OpenOptions::new().append(true).open(path).await?;
            SegmentWriter::with_existing(file, metadata.len())
        } else {
            let path = dir.join(segment_name(1));
            let file = File::create(&path).await?;
            segments.insert(1, path);
            SegmentWriter::new(file)
        };
    
        Ok(WalStore {
            dir: dir.to_path_buf(),
            segments,
            writer,
            max_segment_bytes,
            last_index, 
        })
    }

    pub fn append(&mut self, version: u8, index: u64, term: u64, value_type: u8, payload: &[u8]) {
        self.writer.append(version, index, term, value_type, payload);
        self.last_index = index;
    }
   
    /// rotate() does three things:
    /// 1. Name the new segment — changelog_{last_index + 1}.bin
    /// 2. Create the new file
    /// 3. Replace self.writer with a fresh SegmentWriter
    /// 
    /// The old SegmentWriter is just dropped — its file handle 
    /// closes automatically. The data is already fsynced because 
    /// flush ran before rotate.
    pub async fn flush(&mut self) -> Result<(), WalError> {
        self.writer.flush().await?;

        if self.writer.bytes_written() >= self.max_segment_bytes {
            self.rotate().await?;
        }

        Ok(())
    }

    async fn rotate(&mut self) -> Result<(), WalError> {
        let new_first_index = self.last_index + 1;
        let path = self.dir.join(segment_name(new_first_index));
        let file = File::create(&path).await?;
    
        self.segments.insert(new_first_index, path);
        self.writer = SegmentWriter::new(file);
    
        Ok(())
    }

    /// The goal is: on startup, read back every record ever written, in order. 
    pub async fn replay<F>(&self, mut apply: F) -> Result<(), WalError>
    where
        F: FnMut(u64, u64, Vec<u8>),
    {
        for path in self.segments.values() {
            let data = fs::read(path).await?;
            let mut buf: &[u8] = &data;
   
            // One file can have multiple records, so loop over them.
            // Decoding can throw an error. Propagate it.
            while let Some((index, term, payload)) = record::decode(&mut buf)? {
                
                // We can collect all the records into a vector and then apply them.
                // But this is going to cause memory to blow up. So we accept this apply
                // method that immediately applies the record to the in-memory state.
                // We avoid bloating the memory this way.
                apply(index, term, payload);
            }
        }
    
        Ok(())
    } 

    // TODO: Add a `compact()` method here. That removes the older segments
    // that are not needed.

}

fn segment_name(first_index: u64) -> String {
    format!("changelog_{}.bin", first_index)
}

fn parse_segment_name(name: &str) -> Option<u64> {
    name.strip_prefix("changelog_")?
        .strip_suffix(".bin")?
        .parse()
        .ok()
}
