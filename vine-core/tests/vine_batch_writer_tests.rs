use vine_core::vine_batch_writer::VineBatchWriter;
use vine_core::metadata::{Metadata, MetadataField};
use vine_core::storage_reader::read_vine_data;
use tempfile::tempdir;
use std::fs;

fn create_test_metadata() -> Metadata {
    Metadata::new(
        "test_table",
        vec![
            MetadataField {
                id: 1,
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 2,
                name: "name".to_string(),
                data_type: "string".to_string(),
                is_required: false,
            },
        ],
    )
}

#[test]
fn test_vine_batch_writer_write() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write data
    let rows = vec!["1,Alice", "2,Bob", "3,Charlie"];
    VineBatchWriter::write(base_path, &rows).expect("Failed to write data");

    // Verify data was written
    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], "1,Alice");
    assert_eq!(result[1], "2,Bob");
    assert_eq!(result[2], "3,Charlie");
}

#[test]
fn test_vine_batch_writer_write_empty() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write empty data
    let rows: Vec<&str> = vec![];
    VineBatchWriter::write(base_path, &rows).expect("Failed to write empty data");

    // Verify file was created (even if empty)
    let date_dirs: Vec<_> = fs::read_dir(base_path)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert!(!date_dirs.is_empty());
}

#[test]
fn test_vine_batch_writer_write_missing_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Don't create metadata file
    let rows = vec!["1,Alice"];
    let result = VineBatchWriter::write(base_path, &rows);

    assert!(result.is_err());
}

#[test]
fn test_vine_batch_writer_creates_date_partition() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write data
    let rows = vec!["1,Alice"];
    VineBatchWriter::write(base_path, &rows).expect("Failed to write data");

    // Verify date partition directory was created
    let date_dirs: Vec<_> = fs::read_dir(base_path)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert_eq!(date_dirs.len(), 1);

    // Verify directory name is a valid date (YYYY-MM-DD format)
    let dir_name = date_dirs[0].file_name();
    let dir_name_str = dir_name.to_str().unwrap();
    assert!(dir_name_str.contains('-'));
    assert_eq!(dir_name_str.len(), 10); // YYYY-MM-DD is 10 characters
}

#[test]
fn test_vine_batch_writer_creates_vtx_file() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write data
    let rows = vec!["1,Alice"];
    VineBatchWriter::write(base_path, &rows).expect("Failed to write data");

    // Find the created .vtx file
    let date_dirs: Vec<_> = fs::read_dir(base_path)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert!(!date_dirs.is_empty());

    let date_dir_path = date_dirs[0].path();
    let vtx_files: Vec<_> = fs::read_dir(date_dir_path)
        .expect("Failed to read date dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext == "vtx")
        })
        .collect();

    assert_eq!(vtx_files.len(), 1);

    // Verify filename format (data_HHMMSS_microseconds.vtx)
    let file_name = vtx_files[0].file_name();
    let file_name_str = file_name.to_str().unwrap();
    assert!(file_name_str.starts_with("data_"));
    assert!(file_name_str.ends_with(".vtx"));
}

#[test]
fn test_vine_batch_writer_multiple_writes() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write first batch
    let rows1 = vec!["1,Alice"];
    VineBatchWriter::write(base_path, &rows1).expect("Failed to write first batch");

    // Write second batch
    let rows2 = vec!["2,Bob"];
    VineBatchWriter::write(base_path, &rows2).expect("Failed to write second batch");

    // Verify both batches were written
    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(result.len(), 2);
}
