use crate::metadata::Metadata;
use std::path::PathBuf;

/// Cached metadata to avoid repeated parsing
pub struct WriterCache {
    pub metadata: Metadata,
    pub base_path: PathBuf,
}

impl WriterCache {
    /// Create new cache by loading metadata from path
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let meta_path = base_path.join("vine_meta.json");
        let metadata = Metadata::load(&meta_path)?;

        Ok(Self {
            metadata,
            base_path,
        })
    }

    /// Reload metadata (useful if schema changes)
    pub fn reload(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let meta_path = self.base_path.join("vine_meta.json");
        self.metadata = Metadata::load(&meta_path)?;
        Ok(())
    }

    /// Create cache from explicit metadata (no file needed)
    pub fn from_metadata(
        base_path: PathBuf,
        metadata: Metadata,
    ) -> Self {
        Self {
            metadata,
            base_path,
        }
    }
}
