/// Reads data from Vortex (.vtx) files in date-partitioned directories.
/// Caching is handled internally.

use std::fs;
use std::path::PathBuf;

use chrono::NaiveDate;

use crate::global_cache;
use crate::metadata::Metadata;
use crate::vortex_exp::{read_vortex_file, array_to_csv_rows};

/// Read all data from Vine storage
///
/// This is the main entry point for reading Vine data.
/// Caching is handled internally.
///
/// # Arguments
/// * `dir_path` - Base directory containing date-partitioned Vortex files
///
/// # Returns
/// Vector of CSV-formatted row strings
pub fn read_vine_data(dir_path: &str) -> Vec<String> {
    read_vine_data_internal(dir_path)
        .unwrap_or_else(|e| {
            eprintln!("Error reading Vine data: {}", e);
            Vec::new()
        })
}

/// Internal implementation with automatic caching
fn read_vine_data_internal(dir_path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Use global cache to get metadata
    let metadata = global_cache::get_reader_metadata(dir_path)?;
    read_with_metadata(dir_path, &metadata)
}

/// Read data using provided metadata
fn read_with_metadata(dir_path: &str, metadata: &Metadata) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let base_path = PathBuf::from(dir_path);

    let mut all_rows = Vec::new();
    let mut directories = Vec::new();

    // Scan for date-partitioned directories
    let dir_entries = fs::read_dir(&base_path)?;

    for entry_result in dir_entries {
        let entry = entry_result?;
        let path = entry.path();

        if path.is_dir() {
            if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                if let Ok(date) = NaiveDate::parse_from_str(dir_name, "%Y-%m-%d") {
                    directories.push((date, path));
                }
            }
        }
    }

    // Sort directories by date (chronological order)
    directories.sort_by_key(|(date, _)| *date);

    // Read all Vortex files from date directories
    for (_, dir_path) in directories {
        let sub_dir = fs::read_dir(&dir_path)?;

        for file_entry_result in sub_dir {
            let file_path = file_entry_result?.path();

            // Process .vtx files only
            if file_path.extension().map_or(false, |ext| ext == "vtx") {
                if let Err(e) = read_vortex_file_to_rows(&file_path, metadata, &mut all_rows) {
                    eprintln!("Warning: Failed to read file {:?}: {}", file_path, e);
                }
            }
        }
    }

    Ok(all_rows)
}

/// Read single Vortex file and append rows to row_list
fn read_vortex_file_to_rows(
    file_path: &PathBuf,
    metadata: &Metadata,
    row_list: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_, array) = read_vortex_file(file_path)
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let rows = array_to_csv_rows(&array, metadata)
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    row_list.extend(rows);
    Ok(())
}
