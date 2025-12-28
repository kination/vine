/// Provides configuration for write operations.
/// Vortex handles compression automatically, so only row limits are configurable.

/// Configuration for Vine writers
#[derive(Clone, Debug)]
pub struct WriterConfig {
    /// Maximum rows per file before rotation
    pub max_rows_per_file: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            max_rows_per_file: 100_000,
        }
    }
}

impl WriterConfig {
    /// Create configuration with custom max rows per file
    pub fn with_max_rows(max_rows_per_file: usize) -> Self {
        Self { max_rows_per_file }
    }
}
