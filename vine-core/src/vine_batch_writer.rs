use std::path::{Path, PathBuf};

use crate::streaming_writer::StreamingWriter;
use crate::writer_config::WriterConfig;

/// Batch writer for bulk data ingestion
///
/// Optimized to write large amounts of data in single operation.
/// Implemented based on 'StreamingWriter', but provides simplified API for batch job.
///
/// # Example
/// ```no_run
/// use vine_core::vine_batch_writer::VineBatchWriter;
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
        let base_path: PathBuf = PathBuf::from(path.as_ref());
        let mut writer: StreamingWriter = StreamingWriter::new(base_path, config)?;
        writer.write_batch(data)?;
        writer.close()?;
        Ok(())
    }

    /// Write with 'high throughput'
    pub fn write_high_throughput<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        Self::write(path, data, WriterConfig::high_throughput())
    }

    /// Write with 'balanced' settings (default)
    pub fn write_balanced<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        Self::write(path, data, WriterConfig::balanced())
    }

    /// Write with 'high compression'
    pub fn write_high_compression<P: AsRef<Path>>(
        path: P,
        data: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        Self::write(path, data, WriterConfig::high_compression())
    }
}
