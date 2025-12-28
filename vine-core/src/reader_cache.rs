
use crate::metadata::Metadata;
use std::path::PathBuf;

/// Caching reader metadata/schema information
pub struct ReaderCache {
    pub metadata: Metadata,
    pub base_path: PathBuf,
}

impl ReaderCache {
    /// Create new reader cache from base directory
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let meta_path = base_path.join("vine_meta.json");
        let metadata = Metadata::load(&meta_path)?;

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

    /// Create reader cache with schema-on-read fallback
    ///
    /// Tries multiple sources in order:
    /// 1. vine_meta.json (traditional)
    /// 2. _meta/schema.json (cache)
    /// 3. Infer from Vortex files
    pub fn new_with_fallback(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        // Option 1: vine_meta.json (traditional, highest priority)
        let meta_path = base_path.join("vine_meta.json");
        if meta_path.exists() {
            return Self::new(base_path);
        }

        // Option 2: _meta/schema.json (cached schema)
        if let Some(metadata) = Metadata::load_cached(&base_path) {
            // Validate metadata
            if metadata.fields.is_empty() {
                return Err("Cached metadata must have at least one field".into());
            }
            return Ok(Self {
                metadata,
                base_path,
            });
        }

        // Option 3: Infer from Vortex files
        let metadata = Metadata::infer_from_vortex(&base_path)?;

        // Optionally save to cache for future reads (async, non-blocking)
        let cache_path = base_path.clone();
        let metadata_clone = metadata.clone();
        std::thread::spawn(move || {
            let _ = metadata_clone.save_to_cache(&cache_path);
        });

        Ok(Self {
            metadata,
            base_path,
        })
    }
}
