use crate::metadata::Value;
use crate::writer_cache::WriterCache;
use crate::writer_config::WriterConfig;
use chrono::Local;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int32Type};
use parquet::file::writer::SerializedFileWriter;
use std::fs::{self, File};
use std::path::PathBuf;

/// High-performance streaming writer for Vine format
/// Optimized for:
/// - Minimal allocations
/// - Batched writes
/// - Schema caching
/// - Configurable compression
pub struct StreamingWriter {
    cache: WriterCache,
    config: WriterConfig,
    current_file: Option<SerializedFileWriter<File>>,
    current_file_path: Option<PathBuf>,
    current_row_count: usize,
    current_file_size: usize,
}

impl StreamingWriter {
    /// Create new streaming writer
    pub fn new(
        base_path: PathBuf,
        config: WriterConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cache = WriterCache::new(base_path)?;

        Ok(Self {
            cache,
            config,
            current_file: None,
            current_file_path: None,
            current_row_count: 0,
            current_file_size: 0,
        })
    }

    /// Create with default balanced configuration
    pub fn with_defaults(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(base_path, WriterConfig::default())
    }

    /// Write batch of rows (CSV format for now, will optimize later)
    pub fn write_batch(&mut self, rows: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }

        // Parse rows into columnar format
        let field_count = self.cache.metadata.fields.len();
        let mut values: Vec<Vec<Value>> = vec![Vec::with_capacity(rows.len()); field_count];

        for row in rows {
            let values_array: Vec<&str> = row.split(',').map(|s| s.trim()).collect();

            for (i, field) in self.cache.metadata.fields.iter().enumerate() {
                let raw_value = values_array.get(i).unwrap_or(&"");

                let parsed_value = match field.data_type.as_str() {
                    "string" => Value::String(raw_value.to_string()),
                    "integer" => Value::Int(raw_value.parse::<i32>().unwrap_or_default()),
                    "boolean" => Value::Bool(raw_value.parse::<bool>().unwrap_or_default()),
                    "double" => Value::Double(raw_value.parse::<f64>().unwrap_or_default()),
                    _ => continue,
                };
                values[i].push(parsed_value);
            }
        }

        // Write columnar data
        self.write_columnar_data(&values)?;

        Ok(())
    }

    /// Write columnar data to Parquet
    fn write_columnar_data(
        &mut self,
        values: &[Vec<Value>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if values.is_empty() || values[0].is_empty() {
            return Ok(());
        }

        let row_count = values[0].len();

        // Check if we need to rotate file (size-based or row-based)
        if self.should_rotate_file(row_count) {
            self.close_current_file()?;
        }

        // Ensure we have an open file
        if self.current_file.is_none() {
            self.open_new_file()?;
        }

        // Write data
        let writer = self.current_file.as_mut().unwrap();
        let mut row_group_writer = writer.next_row_group()?;

        for (i, field) in self.cache.metadata.fields.iter().enumerate() {
            if let Some(mut col_writer) = row_group_writer.next_column()? {
                match field.data_type.as_str() {
                    "string" => {
                        let string_values: Vec<ByteArray> = values[i]
                            .iter()
                            .filter_map(|v| {
                                if let Value::String(s) = v {
                                    Some(ByteArray::from(s.as_str()))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        col_writer
                            .typed::<ByteArrayType>()
                            .write_batch(&string_values, None, None)?;
                    }
                    "integer" => {
                        let int_values: Vec<i32> = values[i]
                            .iter()
                            .filter_map(|v| {
                                if let Value::Int(i) = v {
                                    Some(*i)
                                } else {
                                    None
                                }
                            })
                            .collect();
                        col_writer
                            .typed::<Int32Type>()
                            .write_batch(&int_values, None, None)?;
                    }
                    "boolean" => {
                        let bool_values: Vec<bool> = values[i]
                            .iter()
                            .filter_map(|v| {
                                if let Value::Bool(b) = v {
                                    Some(*b)
                                } else {
                                    None
                                }
                            })
                            .collect();
                        col_writer
                            .typed::<BoolType>()
                            .write_batch(&bool_values, None, None)?;
                    }
                    "double" => {
                        let double_values: Vec<f64> = values[i]
                            .iter()
                            .filter_map(|v| {
                                if let Value::Double(d) = v {
                                    Some(*d)
                                } else {
                                    None
                                }
                            })
                            .collect();
                        col_writer
                            .typed::<DoubleType>()
                            .write_batch(&double_values, None, None)?;
                    }
                    _ => continue,
                }
                col_writer.close()?;
            }
        }

        row_group_writer.close()?;
        self.current_row_count += row_count;

        Ok(())
    }

    /// Check if we should rotate to a new file
    fn should_rotate_file(&self, new_rows: usize) -> bool {
        if self.current_file.is_none() {
            return false;
        }

        // Rotate if we exceed row group size or estimated file size
        self.current_row_count + new_rows > self.config.row_group_size
            || self.current_file_size > self.config.max_file_size
    }

    /// Open a new Parquet file
    fn open_new_file(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let now = Local::now();
        let date_dir = now.format("%Y-%m-%d").to_string();
        let time_str = now.format("%H%M%S").to_string();
        let micros = now.timestamp_subsec_micros();

        let date_path = self.cache.base_path.join(&date_dir);
        fs::create_dir_all(&date_path)?;

        // Include microseconds for higher throughput scenarios
        let file_name = format!("data_{}_{}.parquet", time_str, micros);
        let file_path = date_path.join(&file_name);
        let file = File::create(&file_path)?;

        let props = self.config.build_properties();
        let writer = SerializedFileWriter::new(file, self.cache.schema.clone(), props)?;

        self.current_file = Some(writer);
        self.current_file_path = Some(file_path);
        self.current_row_count = 0;
        self.current_file_size = 0;

        Ok(())
    }

    /// Close current file
    fn close_current_file(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(writer) = self.current_file.take() {
            writer.close()?;

            // Update file size for rotation logic
            if let Some(path) = &self.current_file_path {
                if let Ok(metadata) = fs::metadata(path) {
                    self.current_file_size = metadata.len() as usize;
                }
            }
        }
        self.current_file_path = None;
        Ok(())
    }

    /// Flush and close writer
    pub fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.close_current_file()?;
        Ok(())
    }

    /// Flush current data (close and reopen file)
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.close_current_file()?;
        Ok(())
    }
}

impl Drop for StreamingWriter {
    fn drop(&mut self) {
        let _ = self.close_current_file();
    }
}
