use std::path::Path;
use uuid::Uuid;

/// Creating a wrapper struct around structs provided by external crates
/// is a standard practice, so that if the API or the struct of the external
/// crate changes tomorrow, we don't have to change it in all places
pub struct ServerUUID(pub Uuid);

impl ServerUUID {
    pub fn load_or_create(storage_path: &Path) -> Self {
        let uuid_file = storage_path.join("uuid");

        // If file exists, read Uuid
        // else create one and write
        if let Some(existing) = std::fs::read_to_string(&uuid_file)
            .ok()
            .and_then(|contents| Uuid::parse_str(contents.trim()).ok())
        {
            return ServerUUID(existing);
        }

        let new_uuid = Uuid::new_v4();
        let _ = std::fs::write(&uuid_file, new_uuid.to_string());
        ServerUUID(new_uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_or_create_round_trips_the_same_uuid() {
        let dir = std::env::temp_dir().join(format!("tinykeeper-uuid-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();

        let first = ServerUUID::load_or_create(&dir);
        let second = ServerUUID::load_or_create(&dir);

        assert_eq!(first.0, second.0);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
