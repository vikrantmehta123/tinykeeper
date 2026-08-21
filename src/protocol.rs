use bytes::Buf;

pub struct RequestHeader {
    pub xid: i32,
    pub opcode: i32,
}

impl RequestHeader {
    // We pass a mutable reference to the slice so we can advance the cursor
    pub fn from_bytes(buf: &mut &[u8]) -> Self {
        let xid = buf.get_i32();
        let opcode = buf.get_i32();
        Self { xid, opcode }
    }
}

pub struct CreateRequest<'a> {
    pub path: &'a str,
    pub data: &'a [u8],
}

impl<'a> CreateRequest<'a> {
    // Notice the `'a` ties the lifetime of the returned struct directly to the input buffer!
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        // 1. Parse Path
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;

        *buf = remaining; // Manually advance the cursor

        // 2. Parse Data
        let data_len = buf.get_i32() as usize;
        let (data, remaining) = buf.split_at(data_len);

        *buf = remaining; // Advance the cursor again

        Some(Self { path, data })
    }
}

pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64, // Note: zxid is 8-bytes!
    pub err: i32,
}
impl ReplyHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Convert our integers to big-endian bytes and append them
        buf.extend_from_slice(&self.xid.to_be_bytes());
        buf.extend_from_slice(&self.zxid.to_be_bytes());
        buf.extend_from_slice(&self.err.to_be_bytes());
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
}
impl<'a> GetDataRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;
        Some(Self { path })
    }
}

pub struct GetDataResponse {
    pub data: Vec<u8>,
}
impl GetDataResponse {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.data.len() as i32).to_be_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }
}

pub struct SetDataRequest<'a> {
    pub path: &'a str,
    pub data: &'a [u8],
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
        Some(Self { path, data })
    }
}

pub struct DeleteRequest<'a> {
    pub path: &'a str,
}
impl<'a> DeleteRequest<'a> {
    pub fn from_bytes(buf: &mut &'a [u8]) -> Option<Self> {
        let path_len = buf.get_i32() as usize;
        let (path_bytes, remaining) = buf.split_at(path_len);
        let path = std::str::from_utf8(path_bytes).ok()?;
        *buf = remaining;
        Some(Self { path })
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
