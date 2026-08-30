use std::collections::{HashMap, HashSet};

use crate::protocol::{SessionId, WatchEventType, WatchNotification};


/// The ZooKeeper protocol defines five types of watches.
/// First two are part of the original protocol, whereas
/// the persistent watches are added in later ZooKeeper versions.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum WatchType {
    Watch,
    ListWatch,
    PersistentWatch,
    PersistentListWatch,
    PersistentRecursiveWatch,
}

/// We need to uniquely identify something that is being watched.
/// We watch one node in a particular type. So the WatchInfo struct
/// captures the path (i.e. znode) and the type of the watch
///
/// This will be used in forward and reverse index.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WatchInfo {
    pub path: String,
    pub watch_type: WatchType,
}

pub struct WatchEvent {
    pub session_id: SessionId,
    pub payload: Vec<u8>,
}

/// Forward index: given a path, which sessions are watching it?
///
/// We keep separate maps for each watch type. That is, persistent watches 
/// have different maps than the one-shot map. This helps when we want to 
/// notify/do cleanup.
///
/// We need an inverse index also to do cleanup.
pub struct WatchState {
    watches: HashMap<String, HashSet<SessionId>>,
    list_watches: HashMap<String, HashSet<SessionId>>,
    persistent_watches: HashMap<String, HashSet<SessionId>>,
    persistent_list_watches: HashMap<String, HashSet<SessionId>>,
    persistent_recursive_watches: HashMap<String, HashSet<SessionId>>,

    /// Reverse index: given a session, which paths is it watching?
    /// Used at cleanup time when a session disconnects.
    sessions_and_watchers: HashMap<SessionId, HashSet<WatchInfo>>,
}

impl WatchState {
    pub fn new() -> Self {
        Self {
            watches: HashMap::new(),
            list_watches: HashMap::new(),
            persistent_watches: HashMap::new(),
            persistent_list_watches: HashMap::new(),
            persistent_recursive_watches: HashMap::new(),
            sessions_and_watchers: HashMap::new(),
        }
    }

    pub fn register(&mut self, session_id: SessionId, path: String, watch_type: WatchType) {
        let forward_map = match watch_type {
            WatchType::Watch => &mut self.watches,
            WatchType::ListWatch => &mut self.list_watches,
            WatchType::PersistentWatch => &mut self.persistent_watches, 
            WatchType::PersistentListWatch => &mut self.persistent_list_watches, 
            WatchType::PersistentRecursiveWatch => &mut self.persistent_recursive_watches,
        };
    
        forward_map.entry(path.clone()).or_default().insert(session_id);
   
        self.sessions_and_watchers.entry(session_id).or_default().insert(WatchInfo { path, watch_type });
    }

    pub fn clear(&mut self, session_id: SessionId) {
        let Some(watch_infos) = self.sessions_and_watchers.remove(&session_id) else {
            return;
        };
    
        for info in watch_infos {
            let forward_map = match info.watch_type {
                WatchType::Watch => &mut self.watches,
                WatchType::ListWatch => &mut self.list_watches, 
                WatchType::PersistentWatch => &mut self.persistent_watches, 
                WatchType::PersistentListWatch => &mut self.persistent_list_watches, 
                WatchType::PersistentRecursiveWatch => &mut self.persistent_recursive_watches,
            };
    
            if let Some(sessions) = forward_map.get_mut(&info.path) {
                sessions.remove(&session_id);
            }
            if forward_map.get(&info.path).is_some_and(|s| s.is_empty()) {
                forward_map.remove(&info.path);
            }
        }
    }
   
    /// Drain one-shot watches from `forward_map` at `lookup_path`, build a
    /// WatchNotification for each session found, and clean up the reverse index.
    fn fire_oneshot(
        forward_map: &mut HashMap<String, HashSet<SessionId>>,
        reverse_map: &mut HashMap<SessionId, HashSet<WatchInfo>>,
        lookup_path: &str,
        notification_event: WatchEventType,
        notification_path: &str,
        watch_type: WatchType,
    ) -> Vec<WatchEvent> {
        let mut events = vec![];
        if let Some(sessions) = forward_map.remove(lookup_path) {
            for session_id in sessions {
                let notification = WatchNotification { event_type: notification_event, path: notification_path };
                events.push(WatchEvent {
                    session_id,
                    payload: notification.to_bytes(),
                });
                if let Some(infos) = reverse_map.get_mut(&session_id) {
                    infos.remove(&WatchInfo { path: lookup_path.to_string(), watch_type });
                }
            }
        }
        events
    }

    /// When a mutation happens (create, setData, delete), the server calls fire with the
    /// path that was modified and what kind of event it was.
    pub fn fire(&mut self, path: &str, event_type: WatchEventType) -> Vec<WatchEvent> {
        let mut events = vec![];

        // 1. One-shot data watches on the exact path
        events.extend(Self::fire_oneshot(
            &mut self.watches, &mut self.sessions_and_watchers,
            path, event_type, path, WatchType::Watch,
        ));

        // 2. One-shot list watches — depends on event type
        let parent = match path.rsplit_once('/') {
            Some(("", _)) => Some("/"),
            Some((p, _)) => Some(p),
            None => None,
        };

        match event_type {
            WatchEventType::Changed => {},
            WatchEventType::Created => {
                if let Some(parent_path) = parent {
                    events.extend(Self::fire_oneshot(
                        &mut self.list_watches, &mut self.sessions_and_watchers,
                        parent_path, WatchEventType::Child, parent_path, WatchType::ListWatch,
                    ));
                }
            },
            WatchEventType::Deleted => {
                events.extend(Self::fire_oneshot(
                    &mut self.list_watches, &mut self.sessions_and_watchers,
                    path, WatchEventType::Child, path, WatchType::ListWatch,
                ));
                if let Some(parent_path) = parent {
                    events.extend(Self::fire_oneshot(
                        &mut self.list_watches, &mut self.sessions_and_watchers,
                        parent_path, WatchEventType::Child, parent_path, WatchType::ListWatch,
                    ));
                }
            },
            WatchEventType::Child => {},
        }

        // TODO: fire persistent_watches, persistent_list_watches, and
        // persistent_recursive_watches here. Same logic as above but without
        // removing from forward/reverse indexes. Deferred to post-v1.

        events
    }
}
