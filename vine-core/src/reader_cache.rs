//! Reader cache for Vine
//!
//! Caches metadata to avoid repeated parsing.

use crate::metadata::Metadata;
use std::path::PathBuf;

/// Cached reader metadata
pub struct ReaderCache {
    pub metadata: Metadata,
    pub base_path: PathBuf,
}

impl ReaderCache {
    /// Create new reader cache from base directory
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let meta_path = base_path.join("vine_meta.json");
        let metadata = Metadata::load(&meta_path)?;

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

    /// Create reader cache with schema-on-read fallback
    ///
    /// Tries multiple sources in order:
    /// 1. vine_meta.json
    /// 2. _meta/schema.json (cached)
    /// 3. Infer from Vortex files
    pub fn new_with_fallback(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        // Option 1: vine_meta.json
        let meta_path = base_path.join("vine_meta.json");
        if meta_path.exists() {
            return Self::new(base_path);
        }

        // Option 2: _meta/schema.json (cached)
        if let Some(metadata) = Metadata::load_cached(&base_path) {
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

        // Save to cache asynchronously
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
