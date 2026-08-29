use std::collections::{BTreeMap, HashMap, HashSet};
use crate::protocol::SessionId;

pub struct SessionExpiryQueue {
    session_to_expiry: HashMap<SessionId, i64>,
    expiry_to_sessions: BTreeMap<i64, HashSet<SessionId>>,
    interval_ms: i64,
}

impl SessionExpiryQueue {
    pub fn new(interval_ms: i64) -> Self {
        Self { session_to_expiry: HashMap::new(), expiry_to_sessions: BTreeMap::new(), interval_ms }
    }

    #[allow(clippy::collapsible_if)]
    pub fn touch(&mut self, id: SessionId, timeout_ms: i64, now: i64) {

        // If the SessionId exists, remove its old expiry and old entry
        if let Some(old_expiry) = self.session_to_expiry.remove(&id) {
            if let Some(bucket) = self.expiry_to_sessions.get_mut(&old_expiry) {
                bucket.remove(&id);
                if bucket.is_empty() {
                    self.expiry_to_sessions.remove(&old_expiry);
                }
            } 
        }

        // Compute the new expiry and insert
        let bucket_key = ((now + timeout_ms + self.interval_ms - 1) / self.interval_ms) * self.interval_ms;
        self.expiry_to_sessions.entry(bucket_key).or_default().insert(id);
        self.session_to_expiry.insert(id, bucket_key);
    }

    /// Delete a session.
    /// Return true if the session existed and we cleaned up, false if it didn't. 
    pub fn remove(&mut self, id: SessionId) -> bool {
        let Some(old_expiry) = self.session_to_expiry.remove(&id) else {
            return false;
        };
        if let Some(bucket) = self.expiry_to_sessions.get_mut(&old_expiry) {
            bucket.remove(&id);
            if bucket.is_empty() {
                self.expiry_to_sessions.remove(&old_expiry);
            }
        }
        true
    }

    pub fn get_expired(&self, now: i64) -> Vec<SessionId> {
        self.expiry_to_sessions
            .range(..=now)
            .flat_map(|(_, bucket)| bucket.iter().copied())
            .collect()
    } 
}
