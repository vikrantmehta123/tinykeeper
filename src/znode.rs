use crate::protocol::Stat;
use std::collections::HashSet;

pub struct Node {
    pub(crate) stat: Stat,
    pub(crate) data: Vec<u8>,
    pub(crate) children: HashSet<String>,
}
