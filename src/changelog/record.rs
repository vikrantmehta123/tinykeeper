//! The WAL Record
//!
//! Each WAL Record on disk has the following format:
//!
//! checksum: u128
//! version: u8
//! index: u64
//! term: u64
//! value_type: u8
//! payload_len: u32
//! payload: [u8]
//!
//! We define the serialization and deserialization here.
//! The knowledge of the headers + payload is stored in this record.rs file.
//! But the knowledge about how interpret the payload and how to interpret/cast
//! the raw index into LogIndex or term into Term(u64), etc types resides at the caller.

use super::WalError;
use bytes::{BufMut, BytesMut};
use cityhash_rs::cityhash_110_128;

struct RecordHeader {
    checksum: u128,
    version: u8,
    index: u64,
    term: u64,
    value_type: u8,
    payload_len: u32,
}

impl RecordHeader {
    const CHECKSUM_SIZE: usize = 16; // Checksum is 16bytes or 128bits
    const SIZE: usize = Self::CHECKSUM_SIZE + 1 + 8 + 8 + 1 + 4;

    const VERSION_OFFSET: usize = Self::CHECKSUM_SIZE;
    const INDEX_OFFSET: usize = Self::VERSION_OFFSET + 1;
    const TERM_OFFSET: usize = Self::INDEX_OFFSET + 8;
    const VALUE_TYPE_OFFSET: usize = Self::TERM_OFFSET + 8;
    const PAYLOAD_LEN_OFFSET: usize = Self::VALUE_TYPE_OFFSET + 1;
}

/// Serialize a WAL record into a byte buffer
pub fn encode(
    buf: &mut BytesMut,
    version: u8,
    index: u64,
    term: u64,
    value_type: u8,
    payload: &[u8],
) {
    let start = buf.len();

    buf.put_u128_le(0);

    let checksum_start = buf.len();

    buf.put_u8(version);
    buf.put_u64_le(index);
    buf.put_u64_le(term);
    buf.put_u8(value_type);
    buf.put_u32_le(payload.len() as u32);
    buf.put_slice(payload);

    let checksum = cityhash_110_128(&buf[checksum_start..]);

    buf[start..start + RecordHeader::CHECKSUM_SIZE].copy_from_slice(&checksum.to_le_bytes());
}

/// We can get three possible results from this method:
///
/// 1. `Ok(Some((index, term, payload)))`: got a complete, valid record
/// 2. `Ok(None)`: not enough bytes for a full record. For example, EoF or process crashed while
///    writing. But nothing to read in both cases. So, no error.
/// 3. `Err(WalError::ChecksumMismatch)`: bytes are there but corrupted. Thus, error.
pub fn decode(buf: &mut &[u8]) -> Result<Option<(u64, u64, Vec<u8>)>, WalError> {
    if buf.len() < RecordHeader::SIZE {
        return Ok(None);
    }

    let payload_len = u32::from_le_bytes(
        buf[RecordHeader::PAYLOAD_LEN_OFFSET..RecordHeader::PAYLOAD_LEN_OFFSET + 4]
            .try_into()
            .unwrap(),
    ) as usize;

    let record_size = RecordHeader::SIZE + payload_len;

    if buf.len() < record_size {
        return Ok(None);
    }

    let stored = u128::from_le_bytes(buf[..RecordHeader::CHECKSUM_SIZE].try_into().unwrap());
    let computed = cityhash_110_128(&buf[RecordHeader::CHECKSUM_SIZE..record_size]);

    if stored != computed {
        return Err(WalError::ChecksumMismatch);
    }

    let index = u64::from_le_bytes(
        buf[RecordHeader::INDEX_OFFSET..RecordHeader::INDEX_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    let term = u64::from_le_bytes(
        buf[RecordHeader::TERM_OFFSET..RecordHeader::TERM_OFFSET + 8]
            .try_into()
            .unwrap(),
    );

    // NOTE: ClickHouse has multiple log entry types- snapshots, noops, regular commands.
    // For that, it has a value type. For the moment, we don't return it. But later,
    // as the project matures, we may want to return the value_type also from
    // this decode method.
    let _value_type = buf[RecordHeader::VALUE_TYPE_OFFSET];
    let payload = buf[RecordHeader::SIZE..record_size].to_vec();

    *buf = &buf[record_size..];

    Ok(Some((index, term, payload)))
}
