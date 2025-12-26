use parquet::{
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedColumnWriter},
    },
    schema::parser::parse_message_type,
};
use parquet::column::writer::{ColumnWriter};
use parquet::record::{Row, RowAccessor};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type, BoolType, DoubleType};
use chrono::Local;

use std::fs::{self, File, read_to_string};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::metadata::{Metadata, MetadataField, Value};
use crate::streaming_writer::StreamingWriter;
use crate::writer_config::WriterConfig;


/// Legacy write function - kept for backward compatibility with JNI interface
/// For high-performance streaming writes, use StreamingWriter or BatchWriter directly
pub fn write_data<P: AsRef<Path>>(path: P, data: &Vec<&str>) -> parquet::errors::Result<()> {
    // Use optimized streaming writer under the hood
    let base_path = PathBuf::from(path.as_ref());
    let mut writer = StreamingWriter::with_defaults(base_path)
        .map_err(|e: Box<dyn std::error::Error>| parquet::errors::ParquetError::General(e.to_string()))?;

    writer.write_batch(data)
        .map_err(|e: Box<dyn std::error::Error>| parquet::errors::ParquetError::General(e.to_string()))?;

    writer.close()
        .map_err(|e: Box<dyn std::error::Error>| parquet::errors::ParquetError::General(e.to_string()))?;

    Ok(())
}

/// High-performance batch writer for streaming data
/// This is the recommended interface for write-optimized workloads
pub struct BatchWriter {
    writer: StreamingWriter,
}

impl BatchWriter {
    /// Create new batch writer with custom configuration
    pub fn new<P: AsRef<Path>>(path: P, config: WriterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let writer = StreamingWriter::new(base_path, config)?;
        Ok(Self { writer })
    }

    /// Create batch writer optimized for maximum throughput
    pub fn high_throughput<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let writer = StreamingWriter::new(base_path, WriterConfig::high_throughput())?;
        Ok(Self { writer })
    }

    /// Create batch writer with balanced settings (default)
    pub fn balanced<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let writer = StreamingWriter::with_defaults(base_path)?;
        Ok(Self { writer })
    }

    /// Write a batch of rows
    pub fn write_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.write_batch(rows)
    }

    /// Flush pending writes
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.flush()
    }

    /// Close writer
    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.close()
    }
}
