/// High-performance streaming writer optimized for continuous data ingestion.
/// Uses Vortex format (.vtx) for storage.
use std::fs;
use std::path::PathBuf;

use chrono::Local;

use crate::global_cache;
use crate::metadata::Metadata;
use crate::vortex_exp::write_vortex_file;
use crate::writer_config::WriterConfig;

/// High-performance streaming writer for Vine format
///
/// Optimized for:
/// - Continuous data streams
/// - Batched writes
/// - Automatic file rotation
///
/// Caching is handled internally.
#[deprecated]
pub struct StreamingWriter {
    base_path: PathBuf,
    metadata: Metadata,
    config: WriterConfig,
    buffer: Vec<String>,
}

#[deprecated]
impl StreamingWriter {
    /// Create new streaming writer
    ///
    /// Uses internal caching for metadata.
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let path_str = base_path.to_str().unwrap_or("");

        // Use global cache to get metadata
        let metadata = global_cache::get_writer_metadata(path_str)?;

        Ok(Self {
            base_path,
            metadata,
            config: WriterConfig::default(),
            buffer: Vec::new(),
        })
    }

    /// Create with custom configuration
    pub fn with_config(base_path: PathBuf, config: WriterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let path_str = base_path.to_str().unwrap_or("");
        let metadata = global_cache::get_writer_metadata(path_str)?;

        Ok(Self {
            base_path,
            metadata,
            config,
            buffer: Vec::new(),
        })
    }

    /// Write batch of rows (CSV format)
    pub fn write_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }

        // Add rows to buffer
        for row in rows {
            self.buffer.push(row.to_string());
        }

        // Check if we should flush
        if self.buffer.len() >= self.config.max_rows_per_file {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Flush buffer to file
    fn flush_buffer(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Create date partition directory
        let now = Local::now();
        let date_str = now.format("%Y-%m-%d").to_string();
        let partition_dir = self.base_path.join(&date_str);
        fs::create_dir_all(&partition_dir)?;

        // Generate filename with microsecond precision
        let time_str = now.format("%H%M%S").to_string();
        let micros = now.timestamp_subsec_micros();
        let file_path = partition_dir.join(format!("data_{}_{}.vtx", time_str, micros));

        // Convert buffer to &str slice for write_vortex_file
        let rows: Vec<&str> = self.buffer.iter().map(|s| s.as_str()).collect();
        write_vortex_file(&file_path, &self.metadata, &rows)
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;

        // Clear buffer
        self.buffer.clear();

        Ok(())
    }

    /// Flush and close writer
    pub fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.flush_buffer()?;
        Ok(())
    }

    /// Flush current data (write to file)
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.flush_buffer()
    }
}

impl Drop for StreamingWriter {
    fn drop(&mut self) {
        let _ = self.flush_buffer();
    }
}
