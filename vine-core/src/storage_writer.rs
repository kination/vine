use std::path::{Path, PathBuf};

use crate::streaming_writer::StreamingWriter;
use crate::writer_config::WriterConfig;

/// Batch writer for bulk data ingestion
///
/// Optimized for writing large amounts of data in a single operation.
/// Internally uses StreamingWriter but provides simplified API for batch scenarios.
///
/// # Example
/// ```rust
/// let data = vec!["1,alice", "2,bob", "3,charlie"];
/// VineBatchWriter::write("/data/users", &data, WriterConfig::balanced())?;
/// ```
pub struct VineBatchWriter;

impl VineBatchWriter {
    /// Write all data at once with specified configuration
    pub fn write<P: AsRef<Path>>(
        path: P,
        data: &[&str],
        config: WriterConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let mut writer = StreamingWriter::new(base_path, config)?;
        writer.write_batch(data)?;
        writer.close()?;
        Ok(())
    }

    /// Write with high throughput settings
    pub fn write_high_throughput<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        Self::write(path, data, WriterConfig::high_throughput())
    }

    /// Write with balanced settings (default)
    pub fn write_balanced<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        Self::write(path, data, WriterConfig::balanced())
    }

    /// Write with high compression settings
    pub fn write_high_compression<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        Self::write(path, data, WriterConfig::high_compression())
    }
}

/// Streaming writer for incremental data ingestion
///
/// Optimized for continuous data streams where batches arrive over time.
/// Supports explicit control over flushing and file rotation.
///
/// # Example
/// ```rust
/// let mut writer = VineStreamingWriter::new("/data/events", WriterConfig::balanced())?;
///
/// loop {
///     let batch = receive_data();
///     if batch.is_empty() { break; }
///
///     writer.append_batch(&batch)?;
///
///     if should_flush() {
///         writer.flush()?;
///     }
/// }
///
/// writer.close()?;
/// ```
pub struct VineStreamingWriter {
    inner: StreamingWriter,
}

impl VineStreamingWriter {
    /// Create new streaming writer with custom configuration
    pub fn new<P: AsRef<Path>>(
        path: P,
        config: WriterConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let writer = StreamingWriter::new(base_path, config)?;
        Ok(Self { inner: writer })
    }

    /// Create with high throughput configuration
    pub fn high_throughput<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, WriterConfig::high_throughput())
    }

    /// Create with balanced configuration (default)
    pub fn balanced<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, WriterConfig::balanced())
    }

    /// Create with high compression configuration
    pub fn high_compression<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, WriterConfig::high_compression())
    }

    /// Append a batch of rows to the stream
    pub fn append_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.write_batch(rows)
    }

    /// Flush pending writes (closes current file, opens new on next write)
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.flush()
    }

    /// Close the writer and finalize all pending writes
    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.close()
    }
}

/// Legacy API for backward compatibility with existing JNI interface
///
/// This function is kept for compatibility with the original Spark/Trino connectors.
/// New code should use VineBatchWriter or VineStreamingWriter directly.
pub fn write_data<P: AsRef<Path>>(path: P, data: &Vec<&str>) -> parquet::errors::Result<()> {
    VineBatchWriter::write_balanced(path, data)
        .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_metadata(dir: &Path) -> std::io::Result<()> {
        let metadata = r#"{
  "table_name": "test_table",
  "fields": [
    {
      "id": 1,
      "name": "id",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "name",
      "data_type": "string",
      "is_required": true
    }
  ]
}"#;
        fs::write(dir.join("vine_meta.json"), metadata)
    }

    #[test]
    fn test_batch_writer_balanced() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice", "2,bob", "3,charlie"];

        let result = VineBatchWriter::write_balanced(path, &data);
        assert!(result.is_ok(), "Batch write should succeed");

        // Verify files were created
        let entries: Vec<_> = fs::read_dir(path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();

        assert!(!entries.is_empty(), "Should create date directory");
    }

    #[test]
    fn test_batch_writer_high_throughput() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice", "2,bob"];

        let result = VineBatchWriter::write_high_throughput(path, &data);
        assert!(result.is_ok(), "High throughput write should succeed");
    }

    #[test]
    fn test_batch_writer_high_compression() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice"];

        let result = VineBatchWriter::write_high_compression(path, &data);
        assert!(result.is_ok(), "High compression write should succeed");
    }

    #[test]
    fn test_streaming_writer_balanced() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let mut writer = VineStreamingWriter::balanced(path).unwrap();

        // Write first batch
        let batch1 = vec!["1,alice", "2,bob"];
        assert!(writer.append_batch(&batch1).is_ok());

        // Write second batch
        let batch2 = vec!["3,charlie", "4,dave"];
        assert!(writer.append_batch(&batch2).is_ok());

        // Close writer
        assert!(writer.close().is_ok());
    }

    #[test]
    fn test_streaming_writer_flush() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let mut writer = VineStreamingWriter::balanced(path).unwrap();

        // Write and flush
        let batch1 = vec!["1,alice"];
        writer.append_batch(&batch1).unwrap();
        assert!(writer.flush().is_ok());

        // Write again after flush
        let batch2 = vec!["2,bob"];
        writer.append_batch(&batch2).unwrap();

        writer.close().unwrap();
    }

    #[test]
    fn test_streaming_writer_high_throughput() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let mut writer = VineStreamingWriter::high_throughput(path).unwrap();

        for i in 0..10 {
            let batch = vec![format!("{},user{}", i, i)];
            let batch_refs: Vec<&str> = batch.iter().map(|s| s.as_str()).collect();
            writer.append_batch(&batch_refs).unwrap();
        }

        writer.close().unwrap();
    }

    #[test]
    fn test_legacy_write_data() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice", "2,bob"];

        let result = write_data(path, &data);
        assert!(result.is_ok(), "Legacy write_data should work");
    }

    #[test]
    fn test_empty_batch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let empty: Vec<&str> = vec![];

        // Batch writer with empty data
        let result = VineBatchWriter::write_balanced(path, &empty);
        assert!(result.is_ok(), "Empty batch should not fail");
    }

    #[test]
    fn test_large_batch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        // Generate large batch
        let large_data: Vec<String> = (0..1000)
            .map(|i| format!("{},user{}", i, i))
            .collect();
        let large_data_refs: Vec<&str> = large_data.iter().map(|s| s.as_str()).collect();

        let result = VineBatchWriter::write_balanced(path, &large_data_refs);
        assert!(result.is_ok(), "Large batch should succeed");
    }

    #[test]
    fn test_multiple_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let mut writer = VineStreamingWriter::balanced(path).unwrap();

        for _ in 0..3 {
            let batch = vec!["1,test"];
            writer.append_batch(&batch).unwrap();
            writer.flush().unwrap();
        }

        writer.close().unwrap();
    }

    #[test]
    fn test_missing_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        // Don't create metadata

        let data = vec!["1,alice"];

        let result = VineBatchWriter::write_balanced(path, &data);
        assert!(result.is_err(), "Should fail without metadata");
    }
}
