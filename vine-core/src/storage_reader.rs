use std::fs::{self, File};
use std::path::PathBuf;
use chrono::NaiveDate;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

use crate::reader_cache::ReaderCache;

/// Read all data from Vine format storage
///
/// This function:
/// 1. Loads and caches metadata once
/// 2. Scans date-partitioned directories in chronological order
/// 3. Reads Parquet files with proper type handling
/// 4. Returns CSV-formatted rows for JNI compatibility
pub fn read_vine_data(dir_path: &str) -> Vec<String> {
    let base_path = PathBuf::from(dir_path);

    // Initialize reader cache (loads metadata once)
    let cache = ReaderCache::new(base_path.clone())
        .unwrap_or_else(|e| panic!("Failed to initialize reader cache: {}", e));

    let mut row_list = Vec::new();
    let mut directories = Vec::new();

    // Scan for date-partitioned directories
    let dir_entries = fs::read_dir(&base_path)
        .unwrap_or_else(|_| panic!("Cannot read directory: {:?}", base_path));

    for entry_result in dir_entries {
        let entry = entry_result.expect("Cannot read directory entry");
        let path = entry.path();

        if path.is_dir() {
            if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                // Try to parse as date (YYYY-MM-DD format)
                if let Ok(date) = NaiveDate::parse_from_str(dir_name, "%Y-%m-%d") {
                    directories.push((date, path));
                }
            }
        }
    }

    // Sort directories by date (chronological order)
    directories.sort_by_key(|(date, _)| *date);

    // Read all Parquet files from date directories
    for (_, dir_path) in directories {
        let sub_entries = fs::read_dir(&dir_path)
            .unwrap_or_else(|_| panic!("Cannot read directory: {:?}", dir_path));

        for file_entry_result in sub_entries {
            let file_path = file_entry_result
                .expect("Cannot read file entry")
                .path();

            // Process only Parquet files
            if file_path.extension().map_or(false, |ext| ext == "parquet") {
                if let Err(e) = read_parquet_file(&file_path, &cache, &mut row_list) {
                    eprintln!("Warning: Failed to read file {:?}: {}", file_path, e);
                    // Continue reading other files even if one fails
                }
            }
        }
    }

    row_list
}

/// Read a single Parquet file and append rows to row_list
fn read_parquet_file(
    file_path: &PathBuf,
    cache: &ReaderCache,
    row_list: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let row_iter = reader.get_row_iter(None)?;

    for row_result in row_iter {
        match row_result {
            Ok(row) => {
                // Validate column count matches metadata
                let actual_columns = row.len();
                if let Err(e) = cache.validate_column_count(actual_columns) {
                    eprintln!("Warning in file {:?}: {}", file_path, e);
                    continue;
                }

                let csv_row = parse_row_to_csv(&row, cache)?;
                row_list.push(csv_row);
            }
            Err(e) => {
                eprintln!("Warning: Failed to read row from {:?}: {}", file_path, e);
                continue;
            }
        }
    }

    Ok(())
}

/// Parse a Parquet row to CSV format based on metadata schema
///
/// IMPORTANT: Uses field order from metadata (not field.id) to ensure
/// consistency with writer's schema generation
fn parse_row_to_csv(
    row: &parquet::record::Row,
    cache: &ReaderCache,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut values = Vec::with_capacity(cache.field_count());

    // Iterate fields in metadata order (matches writer's schema order)
    for (col_index, field) in cache.metadata.fields.iter().enumerate() {
        let value = match field.data_type.as_str() {
            "integer" => {
                match row.get_int(col_index) {
                    Ok(v) => v.to_string(),
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to read integer column '{}' at index {}: {}. Using default.",
                            field.name, col_index, e
                        );
                        "0".to_string()
                    }
                }
            }
            "string" => {
                match row.get_string(col_index) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to read string column '{}' at index {}: {}. Using empty string.",
                            field.name, col_index, e
                        );
                        String::new()
                    }
                }
            }
            "boolean" => {
                match row.get_bool(col_index) {
                    Ok(v) => v.to_string(),
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to read boolean column '{}' at index {}: {}. Using default.",
                            field.name, col_index, e
                        );
                        "false".to_string()
                    }
                }
            }
            "double" => {
                match row.get_double(col_index) {
                    Ok(v) => v.to_string(),
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to read double column '{}' at index {}: {}. Using default.",
                            field.name, col_index, e
                        );
                        "0.0".to_string()
                    }
                }
            }
            _ => {
                eprintln!(
                    "Warning: Unsupported data type '{}' for field '{}'. Using empty string.",
                    field.data_type, field.name
                );
                String::new()
            }
        };

        values.push(value);
    }

    Ok(values.join(","))
}
