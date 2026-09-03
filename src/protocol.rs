use bytes::Buf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpCode {
    Close = -11,
    Create = 1,
    Remove = 2,
    Exists = 3,
    Get = 4,
    Set = 5,
    GetACL = 6,
    SetACL = 7,
    SimpleList = 8,
    Sync = 9,
    Heartbeat = 11,
    List = 12,
    Check = 13,
    Multi = 14,
    Create2 = 15,
    Reconfig = 16,
    CheckWatch = 17,
    RemoveWatch = 18,
    MultiRead = 22,
    Auth = 100,
    SetWatch = 101,
    SetWatch2 = 105,
    AddWatch = 106,
}

impl TryFrom<i32> for OpCode {
    type Error = i32;

    fn try_from(value: i32) -> Result<OpCode, i32> {
        match value {
            -11 => Ok(OpCode::Close),
            1 => Ok(OpCode::Create),
            2 => Ok(OpCode::Remove),
            3 => Ok(OpCode::Exists),
            4 => Ok(OpCode::Get),
            5 => Ok(OpCode::Set),
            6 => Ok(OpCode::GetACL),
            7 => Ok(OpCode::SetACL),
            8 => Ok(OpCode::SimpleList),
            9 => Ok(OpCode::Sync),
            11 => Ok(OpCode::Heartbeat),
            12 => Ok(OpCode::List),
            13 => Ok(OpCode::Check),
            14 => Ok(OpCode::Multi),
            15 => Ok(OpCode::Create2),
            16 => Ok(OpCode::Reconfig),
            17 => Ok(OpCode::CheckWatch),
            18 => Ok(OpCode::RemoveWatch),
            22 => Ok(OpCode::MultiRead),
            100 => Ok(OpCode::Auth),
            101 => Ok(OpCode::SetWatch),
            105 => Ok(OpCode::SetWatch2),
            106 => Ok(OpCode::AddWatch),
            other => Err(other),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Ok = 0,
    SystemError = -1,
    RuntimeInconsistency =  -2,
    BadArguments = -8,
    NoNode = -101,
    BadVersion = -103,
    NotEmpty = -111,
    NodeExists = -110,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SessionId(pub i64);

impl SessionId {
    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// The ZooKeeper wire format specifies a Stat struct
/// The fields in the Stat struct need to be in a specified
/// order. Those fields are defined below in their appropriate order
/// Note that there are two more fields called data_length and
/// num_children in the protocol. We don't define them in the struct.
/// data_length: i32,     // byte length of node's data
/// num_childnre: i32     // number of direct children
/// Those fields can be computed- so they are not defined here.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stat {
    pub czxid: i64,    // creation zxid
    pub mzxid: i64,    // last modified zxid
    pub ctime: i64,    // creation time (in ms since epoch)
    pub mtime: i64,    // last modification time (ms since epoch)
    pub version: i32,  // How many times has data been written?
    pub cversion: i32, // how many times have children changed?
    // This is also used as seq_num for sequential nodes
    pub aversion: i32,              // how many times did ACL change?
    pub ephemeral_owner: SessionId, // session_id if ephemeral node, else 0

    // data_length and num_children go here, before pzxid
    pub pzxid: i64, // zxid of the last children change
}

impl Stat {
    pub fn to_bytes(&self, data_length: i32, num_children: i32) -> Vec<u8> {
        // As per the protocol, the Stat is always 68 bytes
        let mut buf: Vec<u8> = Vec::with_capacity(68);
        buf.extend_from_slice(&self.czxid.to_be_bytes());
        buf.extend_from_slice(&self.mzxid.to_be_bytes());
        buf.extend_from_slice(&self.ctime.to_be_bytes());
        buf.extend_from_slice(&self.mtime.to_be_bytes());
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.cversion.to_be_bytes());
        buf.extend_from_slice(&self.aversion.to_be_bytes());
        buf.extend_from_slice(&self.ephemeral_owner.to_be_bytes());
        buf.extend_from_slice(&data_length.to_be_bytes());
        buf.extend_from_slice(&num_children.to_be_bytes());
        buf.extend_from_slice(&self.pzxid.to_be_bytes());

        buf
    }
}

pub struct RequestHeader {
    pub xid: i32,
    pub opcode: OpCode,
}

impl RequestHeader {
    pub fn from_bytes(buf: &mut &[u8]) -> Result<Self, i32> {
        let xid = buf.get_i32();
        let raw_opcode = buf.get_i32();
        let opcode = OpCode::try_from(raw_opcode)?;
        Ok(Self { xid, opcode })
    }
}

pub struct CreateRequest<'a> {
    pub path: &'a str,
    pub data: &'a [u8],
    pub flags: i32,
}

impl<'a> CreateRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        // 1. Parse Path
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;

        // 2. Parse Data
        let data_len = buf.get_i32() as usize;
        let (data, remaining) = buf.split_at(data_len);
        *buf = remaining;

        // 3. Skip ACL list
        let acl_count = buf.get_i32();
        for _ in 0..acl_count {
            let _perms = buf.get_i32();
            let scheme_len = buf.get_i32() as usize;
            buf.advance(scheme_len);
            let id_len = buf.get_i32() as usize;
            buf.advance(id_len);
        }

        // 4. Read flags
        let flags = buf.get_i32();

        Some(Self { path, data, flags })
    }
}

pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64, // Note: zxid is 8-bytes!
    pub err: ErrorCode,
}
impl ReplyHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Convert our integers to big-endian bytes and append them
        buf.extend_from_slice(&self.xid.to_be_bytes());
        buf.extend_from_slice(&self.zxid.to_be_bytes());
        buf.extend_from_slice(&(self.err as i32).to_be_bytes());
        buf
    }
}

pub struct CreateResponse<'a> {
    pub path: &'a str,
}
impl<'a> CreateResponse<'a> {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Jute strings must start with their 4-byte length!
        let path_len = self.path.len() as i32;
        buf.extend_from_slice(&path_len.to_be_bytes());

        // Then append the actual utf-8 text bytes
        buf.extend_from_slice(self.path.as_bytes());
        buf
    }
}

pub struct EmptyResponse;
impl EmptyResponse {
    pub fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
}

pub struct GetDataRequest<'a> {
    pub path: &'a str,
    pub watch: bool,
}
impl<'a> GetDataRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;

        let watch = buf.get_u8() != 0;

        Some(Self { path, watch })
    }
}

pub struct GetDataResponse<'a> {
    pub data: &'a [u8],
    pub stat: &'a Stat,
}

impl<'a> GetDataResponse<'a> {
    pub fn to_bytes(&self, num_children: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.data.len() as i32).to_be_bytes());
        buf.extend_from_slice(self.data);

        buf.extend(self.stat.to_bytes(self.data.len() as i32, num_children));
        buf
    }
}

pub struct SetDataRequest<'a> {
    pub path: &'a str,
    pub data: &'a [u8],
    pub version: i32,
}
impl<'a> SetDataRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;

        let data_len = buf.get_i32() as usize;
        let (data, remaining) = buf.split_at(data_len);
        *buf = remaining;

        let version = buf.get_i32();
        Some(Self {
            path,
            data,
            version,
        })
    }
}

pub struct SetDataResponse<'a> {
    pub stat: &'a Stat,
}

impl<'a> SetDataResponse<'a> {
    pub fn to_bytes(&self, data_length: i32, num_children: i32) -> Vec<u8> {
        self.stat.to_bytes(data_length, num_children)
    }
}

pub struct DeleteRequest<'a> {
    pub path: &'a str,
    pub version: i32,
}
impl<'a> DeleteRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;

        let version = buf.get_i32();
        Some(Self { path, version })
    }
}

pub struct ConnectRequest {
    pub protocol_version: i32,
    pub last_zxid_seen: i64,
    pub timeout_ms: i32,
    pub session_id: i64,
    pub password: Vec<u8>,
}

impl ConnectRequest {
    pub fn from_bytes(buf: &mut &[u8]) -> Option<Self> {
        let protocol_version = buf.get_i32();
        let last_zxid_seen = buf.get_i64();
        let timeout_ms = buf.get_i32();
        let session_id = buf.get_i64();

        let password_len = buf.get_i32() as usize;
        if buf.len() < password_len {
            return None;
        }
        let (password_bytes, remaining) = buf.split_at(password_len);
        let password = password_bytes.to_vec();
        *buf = remaining;

        Some(Self {
            protocol_version,
            last_zxid_seen,
            timeout_ms,
            session_id,
            password,
        })
    }
}

pub struct ConnectResponse {
    pub protocol_version: i32,
    pub timeout_ms: i32,
    pub session_id: i64,
    pub password: Vec<u8>,
}

impl ConnectResponse {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.protocol_version.to_be_bytes());
        buf.extend_from_slice(&self.timeout_ms.to_be_bytes());
        buf.extend_from_slice(&self.session_id.to_be_bytes());
        buf.extend_from_slice(&(self.password.len() as i32).to_be_bytes());
        buf.extend_from_slice(&self.password);
        buf
    }
}

pub struct GetChildrenRequest<'a> {
    pub path: &'a str,

    // For v1, this is not going to matter much
    // For v2, we will be using this.
    pub watch: bool,
}

impl<'a> GetChildrenRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;
        let watch = buf.get_u8() != 0;

        Some(Self { path, watch })
    }
}

pub struct GetChildrenResponse<'a> {
    pub children: &'a [&'a String],
}

impl<'a> GetChildrenResponse<'a> {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.children.len() as i32).to_be_bytes());
        for name in self.children {
            buf.extend_from_slice(&(name.len() as i32).to_be_bytes());
            buf.extend_from_slice(name.as_bytes());
        }
        buf
    }
}

pub struct ExistsRequest<'a> {
    pub path: &'a str,
    pub watch: bool,
}

impl<'a> ExistsRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;
        let watch = buf.get_u8() != 0;

        Some(Self { path, watch })
    }
}

pub struct ExistsResponse<'a> {
    pub stat: &'a Stat,
}

impl<'a> ExistsResponse<'a> {
    pub fn to_bytes(&self, data_length: i32, num_children: i32) -> Vec<u8> {
        self.stat.to_bytes(data_length, num_children)
    }
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub enum WatchEventType {
    Created = 1,
    Deleted = 2,
    Changed = 3,
    Child = 4,
}

pub struct WatchNotification<'a> {
    pub event_type: WatchEventType,
    pub path: &'a str,
}

impl<'a> WatchNotification<'a> {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Hardcoded values for different fields in the response of the
        // watch notification
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // xid = -1 as i32
        buf.extend_from_slice(&(-1i64).to_be_bytes()); // zxid = -1 as i64
        buf.extend_from_slice(&(0i32).to_be_bytes()); // err = 0 as i32
        buf.extend_from_slice(&(self.event_type as i32).to_be_bytes()); // event_type as i32
        buf.extend_from_slice(&(3i32).to_be_bytes()); // state = 3
        let path_len = self.path.len() as i32;
        buf.extend_from_slice(&path_len.to_be_bytes());
        buf.extend_from_slice(self.path.as_bytes());
        buf
    }
}

/// `multi` opcode has several sub-ops.
/// Each of those sub-ops has a separate header
pub struct MultiHeader {
    op_type: i32, // what's the opcode of this sub-op 
    done: bool, // if true, no more ops after this. 
    err: i32, // -1 in requests; carries an error code in responses
}

impl MultiHeader {
    const SIZE: usize = 4 + 1 + 4;

    /// Decodes one nine-byte multi header.
    ///
    /// Returns `None` if the header is truncated or contains an invalid
    /// boolean encoding. The input buffer is unchanged on failure.
    pub fn from_bytes(buf: &mut &[u8]) -> Option<Self> {
        let mut cursor = *buf;

        if cursor.len() < Self::SIZE {
            return None;
        }

        let op_type = cursor.get_i32(); 
        
        let done = match cursor.get_u8() {
            0 => false,
            1 => true,
            _ => return None,
        };

        let err = cursor.get_i32();

        // Advance buffer only if the parsing succeeds
        *buf = cursor;

        Some(MultiHeader { op_type, done, err })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        self.encode_into(&mut buf);
        buf
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.op_type.to_be_bytes());
        buf.push(u8::from(self.done));
        buf.extend_from_slice(&self.err.to_be_bytes());
    }
}

pub struct CheckRequest<'a> {
    pub path: &'a str,
    pub version: i32,
}

impl<'a> CheckRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let mut cursor = *buf;
        if cursor.len() < 4 {
            return None;
        }

        let path_len = usize::try_from(cursor.get_i32()).ok()?;
        if cursor.len() < path_len + 4 {
            return None;
        }
        
        let (path_bytes, remaining) = cursor.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        cursor = remaining;

        let version = cursor.get_i32();

        // Move forward the buffer only when request is successfully parsed
        *buf = cursor;

        Some(Self { path, version })
    }
}

pub enum MultiOp<'a> {
    Create(CreateRequest<'a>),
    Set(SetDataRequest<'a>),
    Delete(DeleteRequest<'a>),
    Check(CheckRequest<'a>),
}

impl<'a> MultiOp<'a> {
    pub fn from_bytes(op_type: i32, buf: &mut &'a [u8]) -> Option<Self> {
        match op_type {
            1 => Some(MultiOp::Create(CreateRequest::from_bytes(buf)?)),
            5 => Some(MultiOp::Set(SetDataRequest::from_bytes(buf)?)),
            2 => Some(MultiOp::Delete(DeleteRequest::from_bytes(buf)?)),
            13 => Some(MultiOp::Check(CheckRequest::from_bytes(buf)?)),
            _ => None
        }
    }
}

pub struct MultiRequest<'a> {
    pub ops: Vec<MultiOp<'a>>,
}

impl<'a> MultiRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let mut cursor = *buf;

        let mut ops = Vec::new();

        loop {
            let header = MultiHeader::from_bytes(&mut cursor)?;
            
            if header.done {
                // A multi request ends with the sentinel header (-1, true, -1).
                if header.op_type != -1 || header.err != -1 {
                    return None;
                }
                break;
            }

            // `err` is unused in request-side operation headers.
            if header.err != -1 {
                return None;
            }

            let op = MultiOp::from_bytes(header.op_type, &mut cursor)?;
            ops.push(op);
        }
        *buf = cursor;
        Some(MultiRequest { ops })
    }
}


/// Owned result of one operation inside a `multi` response.
///
/// Error(Ok) represents an operation that was valid but rolled back because
/// another operation in the transaction failed.
#[derive(Debug, Clone, PartialEq)]
pub enum MultiOpResponse {
    Create {
        path: String,
    },
    Set {
        stat: Stat, 
        data_length: i32, 
        num_children: i32,
    }, 
    Delete,
    Check,

    Error(ErrorCode),
}

impl MultiOpResponse {
    /// serializes one MultiOpResponse directly into a buffer owned by MultiResponse
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Create { path } => {
                let header = MultiHeader {
                    op_type: OpCode::Create as i32,
                    done: false,
                    err: ErrorCode::Ok as i32,
                };

                header.encode_into(buf);

                let path_len = path.len() as i32;
                buf.extend_from_slice(&path_len.to_be_bytes());
                buf.extend_from_slice(path.as_bytes());
            }
            Self::Set {
                stat,
                data_length,
                num_children,
            } => {
                let header = MultiHeader {
                    op_type: OpCode::Set as i32,
                    done: false,
                    err: ErrorCode::Ok as i32,
                };

                header.encode_into(buf);
                buf.extend(stat.to_bytes(*data_length, *num_children));
            }

            Self::Delete => {
                let header = MultiHeader {
                    op_type: OpCode::Remove as i32,
                    done: false,
                    err: ErrorCode::Ok as i32,
                };

                header.encode_into(buf);
            }

            Self::Check => {
                let header = MultiHeader {
                    op_type: OpCode::Check as i32,
                    done: false,
                    err: ErrorCode::Ok as i32,
                };

                header.encode_into(buf);
            }

            // For Error(ErrorCode::Ok), this writes zero in both the 
            // header and body, which Kazoo interprets as a rolled-back operation.
            Self::Error(code) => {
                let error = *code as i32;

                let header = MultiHeader {
                    op_type: -1,
                    done: false,
                    err: error,
                };

                header.encode_into(buf);

                // Error responses repeat the error code in their body.
                buf.extend_from_slice(&error.to_be_bytes());
            }
        }
    }
}

/// Response body for a multi request.
///
/// Entries correspond one-for-one, and in order, with the request operations.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiResponse {
    pub responses: Vec<MultiOpResponse>,
}

impl MultiResponse {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        for response in &self.responses {
            response.encode_into(&mut buf);
        }

        let footer = MultiHeader {
            op_type: -1,
            done: true,
            err: -1,
        };

        footer.encode_into(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_request_response_round_trip() {
        let response = ConnectResponse {
            protocol_version: 0,
            timeout_ms: 4000,
            session_id: 42,
            password: vec![9u8; 16],
        };
        let bytes = response.to_bytes();

        // protocol_version(4) + timeout_ms(4) + session_id(8)
        // + password_len(4) + password(16) = 36 bytes.
        assert_eq!(bytes.len(), 36);

        // Build a real ConnectRequest wire payload directly to test
        // from_bytes properly (it carries an extra field, last_zxid_seen,
        // that ConnectResponse doesn't have).
        let mut request_bytes = Vec::new();
        request_bytes.extend_from_slice(&0i32.to_be_bytes()); // protocol_version
        request_bytes.extend_from_slice(&100i64.to_be_bytes()); // last_zxid_seen
        request_bytes.extend_from_slice(&4000i32.to_be_bytes()); // timeout_ms
        request_bytes.extend_from_slice(&0i64.to_be_bytes()); // session_id (0 = new)
        request_bytes.extend_from_slice(&16i32.to_be_bytes()); // password len
        request_bytes.extend_from_slice(&[0u8; 16]); // password

        let mut buf = request_bytes.as_slice();
        let request = ConnectRequest::from_bytes(&mut buf).unwrap();

        assert_eq!(request.protocol_version, 0);
        assert_eq!(request.last_zxid_seen, 100);
        assert_eq!(request.timeout_ms, 4000);
        assert_eq!(request.session_id, 0);
        assert_eq!(request.password, vec![0u8; 16]);
        assert!(buf.is_empty());
    }
}
