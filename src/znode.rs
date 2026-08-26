use crate::protocol::Stat;
use std::collections::HashMap;

pub struct ZNode {
    pub(crate) stat: Stat,
    pub(crate) data: Vec<u8>,
    pub(crate) children: HashMap<String, ZNode>,
}

impl ZNode {
    pub fn stat_bytes(&self) -> Vec<u8> {
        self.stat
            .to_bytes(self.data.len() as i32, self.children.len() as i32)
    }
}
