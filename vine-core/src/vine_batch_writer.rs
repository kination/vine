use std::fs;
use std::path::{Path, PathBuf};

use chrono::Local;
use vortex::ArrayRef as VortexArrayRef;

use crate::vortex_exp::write_vortex_array;

/// Batch writer for bulk data ingestion
///
/// Writes all data in a single operation using Vortex arrays directly.
/// Caching is handled internally.
pub struct VineBatchWriter;

impl VineBatchWriter {
    /// Write Vortex array directly to storage
    pub fn write<P: AsRef<Path>>(
        path: P,
        array: &VortexArrayRef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base_path: PathBuf = PathBuf::from(path.as_ref());

        // Create date partition directory
        let date_str = Local::now().format("%Y-%m-%d").to_string();
        let partition_dir = base_path.join(&date_str);
        fs::create_dir_all(&partition_dir)?;

        // Generate filename with microsecond precision
        let timestamp = Local::now().format("%H%M%S_%f").to_string();
        let file_path = partition_dir.join(format!("data_{}.vtx", timestamp));

        // Write Vortex file
        write_vortex_array(&file_path, array)
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;

        Ok(())
    }
}
