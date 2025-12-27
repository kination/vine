use std::path::{Path, PathBuf};

use crate::streaming_writer::StreamingWriter;
use crate::writer_config::WriterConfig;

/// Batch writer for bulk data ingestion
///
/// Optimized for writing large amounts of data in a single operation.
/// Internally uses StreamingWriter but provides simplified API for batch scenarios.
///
/// # Example
/// ```no_run
/// use vine_core::storage_writer::VineBatchWriter;
/// use vine_core::writer_config::WriterConfig;
///
/// let data = vec!["1,alice", "2,bob", "3,charlie"];
/// VineBatchWriter::write("/data/users", &data, WriterConfig::balanced())?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
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
/// ```no_run
/// use vine_core::storage_writer::VineStreamingWriter;
/// use vine_core::writer_config::WriterConfig;
///
/// let mut writer = VineStreamingWriter::new("/data/events", WriterConfig::balanced())?;
/// let batches = vec![vec!["1,alice"], vec!["2,bob"]];
///
/// for batch in batches {
///     writer.append_batch(&batch)?;
/// }
///
/// writer.close()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
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