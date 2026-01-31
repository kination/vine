use vine_core::vine_streaming_writer::VineStreamingWriter;
use vine_core::metadata::{Metadata, MetadataField};
use vine_core::writer_config::WriterConfig;
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
fn test_vine_streaming_writer_new() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer
    let writer = VineStreamingWriter::new(base_path);
    assert!(writer.is_ok());
}

#[test]
fn test_vine_streaming_writer_new_missing_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Don't create metadata file
    let result = VineStreamingWriter::new(base_path);
    assert!(result.is_err());
}

#[test]
fn test_vine_streaming_writer_with_config() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer with custom config
    let config = WriterConfig::with_max_rows(50_000);
    let writer = VineStreamingWriter::with_config(base_path, config);
    assert!(writer.is_ok());
}

#[test]
fn test_vine_streaming_writer_append_batch() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer and append batch
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let rows = vec!["1,Alice", "2,Bob"];
    let result = writer.append_batch(&rows);
    assert!(result.is_ok());
}

#[test]
fn test_vine_streaming_writer_append_multiple_batches() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer and append multiple batches
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");

    let rows1 = vec!["1,Alice"];
    writer.append_batch(&rows1).expect("Failed to append first batch");

    let rows2 = vec!["2,Bob"];
    writer.append_batch(&rows2).expect("Failed to append second batch");

    let rows3 = vec!["3,Charlie"];
    writer.append_batch(&rows3).expect("Failed to append third batch");
}

#[test]
fn test_vine_streaming_writer_flush() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer, append batch, and flush
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let rows = vec!["1,Alice", "2,Bob"];
    writer.append_batch(&rows).expect("Failed to append batch");

    let result = writer.flush();
    assert!(result.is_ok());
}

#[test]
fn test_vine_streaming_writer_close() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer, append batch, and close
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let rows = vec!["1,Alice", "2,Bob"];
    writer.append_batch(&rows).expect("Failed to append batch");

    let result = writer.close();
    assert!(result.is_ok());
}

#[test]
fn test_vine_streaming_writer_write_and_read_roundtrip() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write data using streaming writer
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let rows = vec!["1,Alice", "2,Bob", "3,Charlie"];
    writer.append_batch(&rows).expect("Failed to append batch");
    writer.close().expect("Failed to close writer");

    // Read data back
    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], "1,Alice");
    assert_eq!(result[1], "2,Bob");
    assert_eq!(result[2], "3,Charlie");
}

#[test]
fn test_vine_streaming_writer_flush_multiple_times() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create writer and test multiple flushes
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");

    // First batch and flush
    let rows1 = vec!["1,Alice"];
    writer.append_batch(&rows1).expect("Failed to append first batch");
    writer.flush().expect("Failed to flush first time");

    // Second batch and flush
    let rows2 = vec!["2,Bob"];
    writer.append_batch(&rows2).expect("Failed to append second batch");
    writer.flush().expect("Failed to flush second time");

    // Close writer
    writer.close().expect("Failed to close writer");

    // Verify all data was written
    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(result.len(), 2);
}

#[test]
fn test_vine_streaming_writer_creates_date_partition() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Write data
    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let rows = vec!["1,Alice"];
    writer.append_batch(&rows).expect("Failed to append batch");
    writer.close().expect("Failed to close writer");

    // Verify date partition directory was created
    let date_dirs: Vec<_> = fs::read_dir(base_path)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert!(!date_dirs.is_empty());

    // Verify directory name is a valid date (YYYY-MM-DD format)
    let dir_name = date_dirs[0].file_name();
    let dir_name_str = dir_name.to_str().unwrap();
    assert!(dir_name_str.contains('-'));
    assert_eq!(dir_name_str.len(), 10); // YYYY-MM-DD is 10 characters
}
