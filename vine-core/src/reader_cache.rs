use crate::metadata::Metadata;
use std::path::PathBuf;
use std::sync::Arc;

/// Caching 'reader metadata'/'schema information'
/// Prevent frequent metadata parsing and ensures consistency with 'writer'
pub struct ReaderCache {
    pub metadata: Metadata,
    pub base_path: PathBuf,
}

impl ReaderCache {
    /// Create new reader cache from base directory
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let meta_path = base_path.join("vine_meta.json");
        let meta_str = std::fs::read_to_string(&meta_path)
            .map_err(|e| format!("Failed to read metadata from {:?}: {}", meta_path, e))?;

        let metadata: Metadata = serde_json::from_str(&meta_str)
            .map_err(|e| format!("Failed to parse metadata: {}", e))?;

        // Validate metadata
        if metadata.fields.is_empty() {
            return Err("Metadata must have at least one field".into());
        }

        Ok(Self {
            metadata,
            base_path,
        })
    }

    /// Get field count
    pub fn field_count(&self) -> usize {
        self.metadata.fields.len()
    }

    /// Validate that a row has correct number of columns
    pub fn validate_column_count(&self, actual_count: usize) -> Result<(), String> {
        let expected_count = self.field_count();
        if actual_count != expected_count {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                expected_count, actual_count
            ));
        }
        Ok(())
    }
}
