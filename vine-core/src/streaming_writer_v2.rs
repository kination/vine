/// Enhanced streaming writer using Vortex Writer API with chunk accumulation.
///
/// **Improvement from streaming_writer.rs:**
/// This version accumulates ArrayRef chunks in memory and write them together
/// using Writer API in Vortex, which provides:
/// - Better compression (larger chunks)
/// - Single file per flush (vs many small files)
/// - Vortex's optimized write codepath
///
/// But...
/// - Chunks are held in memory until flush. For bounded memory usage,
/// - Need to call `flush()` periodically
use std::fs;
use std::path::PathBuf;

use chrono::Local;
use tokio::runtime::Runtime;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;
use vortex::ArrayRef;
use vortex::arrays::ChunkedArray;
use vortex::IntoArray;

use crate::global_cache;
use crate::metadata::Metadata;
use crate::vortex_exp::{build_struct_array, create_session};
use crate::writer_config::WriterConfig;

/// Summary of a flush operation
#[derive(Debug, Clone)]
pub struct FlushSummary {
    /// Number of bytes written to file
    pub bytes_written: u64,
    /// Number of rows written to file
    pub rows_written: usize,
    /// Path to the file that was created
    pub file_path: PathBuf,
}

/// Enhanced streaming writer with chunk accumulation
///
/// Accumulates chunks in memory and writes them efficiently using
/// Vortex's Writer API when flushing.
///
/// # Example
/// ```ignore
/// let mut writer = StreamingWriterV2::new("/data/events")?;
///
/// // Push chunks as they arrive
/// for i in 0..5 {
///     let rows = generate_batch(i);
///     writer.write_batch(&rows)?;
///
///     // Monitor progress
///     println!("Chunk {}: {} chunks buffered, {} rows total",
///         i + 1, writer.buffered_chunks(), writer.buffered_rows());
/// }
///
/// // Flush to file and get summary
/// let summary = writer.flush()?;
/// println!("✓ Wrote {} bytes, {} rows to {:?}",
///     summary.bytes_written, summary.rows_written, summary.file_path);
///
/// // Continue with more data
/// writer.write_batch(&["100,NextBatch"])?;
/// writer.close()?;
/// ```
pub struct StreamingWriterV2 {
    base_path: PathBuf,
    metadata: Metadata,
    config: WriterConfig,
    session: VortexSession,
    runtime: Runtime,
    chunk_buffer: Vec<ArrayRef>,
    total_bytes_written: u64,
    total_rows_written: usize,
    current_buffer_rows: usize,
}

impl StreamingWriterV2 {
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let path_str = base_path.to_str().unwrap_or("");
        let metadata = global_cache::get_writer_metadata(path_str)?;
        let session = create_session();
        let runtime = Runtime::new()?;

        Ok(Self {
            base_path,
            metadata,
            config: WriterConfig::default(),
            session,
            runtime,
            chunk_buffer: Vec::new(),
            total_bytes_written: 0,
            total_rows_written: 0,
            current_buffer_rows: 0,
        })
    }

    /// Create with custom configuration
    pub fn with_config(
        base_path: PathBuf,
        config: WriterConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path_str = base_path.to_str().unwrap_or("");
        let metadata = global_cache::get_writer_metadata(path_str)?;
        let session = create_session();
        let runtime = Runtime::new()?;

        Ok(Self {
            base_path,
            metadata,
            config,
            session,
            runtime,
            chunk_buffer: Vec::new(),
            total_bytes_written: 0,
            total_rows_written: 0,
            current_buffer_rows: 0,
        })
    }

    /// Write batch of rows (CSV format)
    ///
    /// Convert rows(csv) to Vortex arrays, and accumulated in memory.
    /// `flush()` to write accumulated chunks to disk.
    pub fn write_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }

        // Check if adding current rows would exceed limit
        if self.current_buffer_rows + rows.len() > self.config.max_rows_per_file
            && !self.chunk_buffer.is_empty()
        {
            // Flush before exceeding
            self.flush()?;
        }

        // Convert 'csv' rows to 'Vortex array'
        let array = build_struct_array(&self.metadata, rows)
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;

        self.current_buffer_rows += rows.len();
        self.chunk_buffer.push(array);

        Ok(())
    }

    /// Flush accumulated chunks to a new file
    ///
    /// Combine all buffered chunks into a single ChunkedArray,
    ///  and write to new date-partitioned vortex file.
    ///
    pub fn flush(&mut self) -> Result<Option<FlushSummary>, Box<dyn std::error::Error>> {
        if self.chunk_buffer.is_empty() {
            return Ok(None);
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

        // Get 'DType' from first chunk
        let dtype = self.chunk_buffer[0].dtype().clone();

        // Combine chunks into ChunkedArray
        let chunked_data = if self.chunk_buffer.len() == 1 {
            self.chunk_buffer[0].clone()
        } else {
            ChunkedArray::try_new(self.chunk_buffer.clone(), dtype)
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
                .into_array()
        };

        // Write using Vortex Writer API
        let bytes_written = self.runtime.block_on(async {
            let file = async_fs::File::create(&file_path).await?;
            let summary = self.session
                .write_options()
                .write(file, chunked_data.to_array_stream())
                .await?;
            Ok::<u64, Box<dyn std::error::Error>>(summary.size())
        })?;

        self.total_bytes_written += bytes_written;
        self.total_rows_written += self.current_buffer_rows;

        // Create summary
        let summary = FlushSummary {
            bytes_written,
            rows_written: self.current_buffer_rows,
            file_path: file_path.clone(),
        };

        // Clear buffers
        self.chunk_buffer.clear();
        self.current_buffer_rows = 0;

        Ok(Some(summary))
    }

    /// Flush and close writer
    pub fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.flush()?;
        Ok(())
    }

    /// Get total bytes written across all files
    pub fn bytes_written(&self) -> u64 {
        self.total_bytes_written
    }

    /// Get number of chunks currently buffered (not yet flushed)
    pub fn buffered_chunks(&self) -> usize {
        self.chunk_buffer.len()
    }

    /// Get number of rows currently buffered (not yet flushed)
    pub fn buffered_rows(&self) -> usize {
        self.current_buffer_rows
    }

    /// Get total number of rows written across all files
    pub fn total_rows_written(&self) -> usize {
        self.total_rows_written
    }

    /// Get the base path for this writer
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    /// Get reference to metadata
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

impl Drop for StreamingWriterV2 {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::metadata::{Metadata, MetadataField};

    fn create_test_metadata() -> Metadata {
        Metadata::new(
            "test_stream_v2",
            vec![
                MetadataField {
                    id: 1,
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    is_required: true,
                },
                MetadataField {
                    id: 2,
                    name: "name".to_string(),
                    data_type: "string".to_string(),
                    is_required: false,
                },
            ],
        )
    }

    #[test]
    fn test_streaming_writer_v2_basic() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path();

        let meta_path = path.join("vine_meta.json");
        let metadata = create_test_metadata();
        metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

        let mut writer = StreamingWriterV2::new(path.to_path_buf())
            .expect("Failed to create writer");

        // Write and accumulate
        writer.write_batch(&["1,Alice", "2,Bob"]).expect("Write failed");
        assert_eq!(writer.buffered_rows(), 2);
        assert_eq!(writer.buffered_chunks(), 1);

        writer.write_batch(&["3,Charlie"]).expect("Write failed");
        assert_eq!(writer.buffered_rows(), 3);
        assert_eq!(writer.buffered_chunks(), 2);

        // Flush - should write to file and return summary
        let summary = writer.flush().expect("Flush failed");
        assert!(summary.is_some(), "Should return flush summary");
        let summary = summary.unwrap();

        assert_eq!(summary.rows_written, 3, "Should have written 3 rows");
        assert!(summary.bytes_written > 0, "Should have written bytes");
        assert!(summary.file_path.exists(), "File should exist");

        assert_eq!(writer.buffered_rows(), 0);
        assert_eq!(writer.buffered_chunks(), 0);
        assert!(writer.bytes_written() > 0);

        // Write more (new file)
        writer.write_batch(&["4,Diana"]).expect("Write failed");
        writer.close().expect("Close failed");

        // Verify files
        let date_str = Local::now().format("%Y-%m-%d").to_string();
        let partition_dir = path.join(&date_str);
        assert!(partition_dir.exists());

        let files: Vec<_> = std::fs::read_dir(&partition_dir)
            .expect("Failed to read dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "vtx"))
            .collect();

        assert!(files.len() >= 2, "Should create at least 2 files");
    }

    #[test]
    fn test_auto_flush() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path();

        let meta_path = path.join("vine_meta.json");
        let metadata = create_test_metadata();
        metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

        // Create writer with small max_rows_per_file
        let mut config = WriterConfig::default();
        config.max_rows_per_file = 5;

        let mut writer = StreamingWriterV2::with_config(path.to_path_buf(), config)
            .expect("Failed to create writer");

        // Write 3 rows (no flush yet)
        writer.write_batch(&["1,A", "2,B", "3,C"]).expect("Write failed");
        assert_eq!(writer.buffered_rows(), 3);

        // Write 3 more rows (3+3 > 5, so flushes first 3 data, then add 3)
        writer.write_batch(&["4,D", "5,E", "6,F"]).expect("Write failed");
        assert_eq!(writer.buffered_rows(), 3);

        writer.close().expect("Close failed");
    }

    #[test]
    fn test_empty_flush() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let path = temp_dir.path();

        let meta_path = path.join("vine_meta.json");
        let metadata = create_test_metadata();
        metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

        let mut writer = StreamingWriterV2::new(path.to_path_buf())
            .expect("Failed to create writer");

        // Flush without writing should return None
        let summary = writer.flush().expect("Flush should succeed");
        assert!(summary.is_none(), "Empty flush should return None");
        assert_eq!(writer.bytes_written(), 0);

        writer.close().expect("Close failed");
    }
}
