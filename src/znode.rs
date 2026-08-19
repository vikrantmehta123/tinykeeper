use std::collections::HashMap; 

pub struct ZNode {
    pub(crate) version: u32, 
    pub(crate) data: Vec<u8>, 
    pub(crate) children: HashMap<String, ZNode>
}

impl ZNode {
    pub fn new() -> Self {
        ZNode {
            version: 0u32, 
            data: Vec::new(), 
            children: HashMap::new()
        }
    }
}
