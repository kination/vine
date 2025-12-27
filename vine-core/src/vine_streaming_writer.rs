use std::path::{Path, PathBuf};

use crate::streaming_writer::StreamingWriter;
use crate::writer_config::WriterConfig;

/// Streaming writer for incremental data ingestion
///
/// Optimized on handling 'continuous data streams' where batches arrive over time.
/// Supports explicit control over flushing and file rotation.
///
/// # Example
/// ```no_run
/// use vine_core::vine_streaming_writer::VineStreamingWriter;
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

    /// Create with 'high throughput'
    pub fn high_throughput<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, WriterConfig::high_throughput())
    }

    /// Create with 'balanced' configuration (default)
    pub fn balanced<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, WriterConfig::balanced())
    }

    /// Create with 'high compression'
    pub fn high_compression<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, WriterConfig::high_compression())
    }

    /// Append 'batch of rows' to the stream
    pub fn append_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.write_batch(rows)
    }

    /// Flush pending writes (closes current one, and open new file on next write)
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.flush()
    }

    /// Close the writer and finalize all pending writes
    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.close()
    }
}
