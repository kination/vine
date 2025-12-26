use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{WriterProperties, EnabledStatistics};
use std::sync::Arc;

/// Configuration for write-optimized Parquet writer
#[derive(Clone)]
pub struct WriterConfig {
    /// Compression algorithm (default: SNAPPY for speed)
    pub compression: Compression,

    /// Row group size (default: 100,000 rows for streaming)
    pub row_group_size: usize,

    /// Write buffer size in bytes (default: 64MB)
    pub write_buffer_size: usize,

    /// Enable dictionary encoding for strings (default: true)
    pub enable_dictionary: bool,

    /// Enable statistics (default: false for write speed)
    pub enable_statistics: bool,

    /// Maximum file size in bytes before rolling to new file (default: 256MB)
    pub max_file_size: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            // SNAPPY is fastest, good enough compression for streaming
            compression: Compression::SNAPPY,
            // Optimized for streaming: smaller row groups for faster writes
            row_group_size: 100_000,
            // 64MB write buffer
            write_buffer_size: 64 * 1024 * 1024,
            // Dictionary encoding helps compression without much overhead
            enable_dictionary: true,
            // Skip statistics for write performance
            enable_statistics: false,
            // 256MB max file size for better parallelism on read
            max_file_size: 256 * 1024 * 1024,
        }
    }
}

impl WriterConfig {
    /// Create config optimized for maximum write throughput
    pub fn high_throughput() -> Self {
        Self {
            compression: Compression::UNCOMPRESSED,
            row_group_size: 50_000,
            write_buffer_size: 128 * 1024 * 1024,
            enable_dictionary: false,
            enable_statistics: false,
            max_file_size: 512 * 1024 * 1024,
        }
    }

    /// Create config balanced between write speed and storage efficiency
    pub fn balanced() -> Self {
        Self::default()
    }

    /// Create config optimized for storage efficiency (slower writes)
    pub fn high_compression() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::default()),
            row_group_size: 1_000_000,
            write_buffer_size: 32 * 1024 * 1024,
            enable_dictionary: true,
            enable_statistics: true,
            max_file_size: 128 * 1024 * 1024,
        }
    }

    /// Build Parquet WriterProperties from config
    pub fn build_properties(&self) -> Arc<WriterProperties> {
        let mut builder = WriterProperties::builder()
            .set_compression(self.compression)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .set_write_batch_size(self.row_group_size);

        let stats = if self.enable_statistics {
            EnabledStatistics::Page
        } else {
            EnabledStatistics::None
        };
        builder = builder.set_statistics_enabled(stats);

        if self.enable_dictionary {
            builder = builder.set_dictionary_enabled(true);
        }

        Arc::new(builder.build())
    }
}
