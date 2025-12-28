use std::fs;
use std::path::{Path, PathBuf};

use chrono::Local;

use crate::global_cache;
use crate::vortex_exp::write_vortex_file;

/// Batch writer for bulk data ingestion
///
/// Writes all data in a single operation.
/// Caching is handled internally.
pub struct VineBatchWriter;

impl VineBatchWriter {
    /// Write all data at once
    pub fn write<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base_path: PathBuf = PathBuf::from(path.as_ref());
        let path_str = base_path.to_str().unwrap_or("");

        // Use global cache to get metadata
        let metadata = global_cache::get_writer_metadata(path_str)?;

        // Create date partition directory
        let date_str = Local::now().format("%Y-%m-%d").to_string();
        let partition_dir = base_path.join(&date_str);
        fs::create_dir_all(&partition_dir)?;

        // Generate filename with microsecond precision
        let timestamp = Local::now().format("%H%M%S_%f").to_string();
        let file_path = partition_dir.join(format!("data_{}.vtx", timestamp));

        // Write Vortex file
        write_vortex_file(&file_path, &metadata, data)
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;

        Ok(())
    }
}
