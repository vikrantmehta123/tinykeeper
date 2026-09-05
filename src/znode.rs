use crate::protocol::Stat;
use std::collections::HashSet;

#[derive(Clone)]
pub struct Node {
    pub(crate) stat: Stat,
    pub(crate) data: Vec<u8>,
    pub(crate) children: HashSet<String>,
}
