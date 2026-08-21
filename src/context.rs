use std::sync::{Arc, OnceLock};

use crate::config::Config;
use crate::dispatcher::KeeperDispatcher;
use crate::server_uuid::ServerUUID;

pub struct KeeperContext {
    pub config: Config,
    pub uuid: ServerUUID,

    // KeeperDispatcher has an async operation
    // OnceLock puts an empty stub at the time of init but later adds the
    // result once it is returned from the async function.
    // Otherwise, we would need to await it here.
    pub dispatcher: OnceLock<Arc<KeeperDispatcher>>,
}
