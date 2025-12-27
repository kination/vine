use std::path::Path;

use crate::vine_batch_writer::VineBatchWriter;

/// Legacy API for backward compatibility with existing JNI interface
///
/// This function is kept for compatibility with the original Spark/Trino connectors.
/// New code should use VineBatchWriter or VineStreamingWriter directly.
pub fn write_data<P: AsRef<Path>>(path: P, data: &Vec<&str>) -> parquet::errors::Result<()> {
    VineBatchWriter::write_balanced(path, data)
        .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))
}