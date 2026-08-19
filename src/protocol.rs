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
