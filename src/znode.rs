use crate::protocol::Stat;
use std::collections::HashMap;

pub struct ZNode {
    pub(crate) stat: Stat,
    pub(crate) data: Vec<u8>,
    pub(crate) children: HashMap<String, ZNode>,
}
