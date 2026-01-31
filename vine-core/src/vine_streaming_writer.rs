use std::path::{Path, PathBuf};

// use crate::streaming_writer::StreamingWriter;
use crate::streaming_writer_v2::StreamingWriterV2 as StreamingWriter;
use crate::writer_config::WriterConfig;


pub struct VineStreamingWriter {
    inner: StreamingWriter,
}

impl VineStreamingWriter {
    /// Create new streaming writer with default configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let writer = StreamingWriter::new(base_path)?;
        Ok(Self { inner: writer })
    }

    /// Create with custom configuration
    pub fn with_config<P: AsRef<Path>>(
        path: P,
        config: WriterConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = PathBuf::from(path.as_ref());
        let writer = StreamingWriter::with_config(base_path, config)?;
        Ok(Self { inner: writer })
    }

    /// Append batch of rows to the stream
    pub fn append_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.write_batch(rows)
    }

    /// Flush pending writes (closes current file, opens new file on next write)
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.flush()?;
        Ok(())
    }

    /// Close the writer and finalize all pending writes
    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.close()
    }
}
