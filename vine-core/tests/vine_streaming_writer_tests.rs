use vine_core::vine_streaming_writer::VineStreamingWriter;
use vine_core::metadata::{Metadata, MetadataField};
use vine_core::writer_config::WriterConfig;
use vine_core::storage_reader::read_vine_data;
use vine_core::vortex_exp::build_struct_array;
use vine_core::arrow_bridge::vortex_to_arrow;
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

/// Helper: build VortexArrayRef from comma-separated rows
fn build_test_array(metadata: &Metadata, rows: &[&str]) -> vortex::ArrayRef {
    build_struct_array(metadata, rows).expect("Failed to build test array")
}

#[test]
fn test_vine_streaming_writer_new() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let writer = VineStreamingWriter::new(base_path);
    assert!(writer.is_ok());
}

#[test]
fn test_vine_streaming_writer_new_missing_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let result = VineStreamingWriter::new(base_path);
    assert!(result.is_err());
}

#[test]
fn test_vine_streaming_writer_with_config() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let config = WriterConfig::with_max_rows(50_000);
    let writer = VineStreamingWriter::with_config(base_path, config);
    assert!(writer.is_ok());
}

#[test]
fn test_vine_streaming_writer_append_batch() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let array = build_test_array(&metadata, &["1,Alice", "2,Bob"]);
    let result = writer.append_batch(&array);
    assert!(result.is_ok());
}

#[test]
fn test_vine_streaming_writer_append_multiple_batches() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");

    let array1 = build_test_array(&metadata, &["1,Alice"]);
    writer.append_batch(&array1).expect("Failed to append first batch");

    let array2 = build_test_array(&metadata, &["2,Bob"]);
    writer.append_batch(&array2).expect("Failed to append second batch");

    let array3 = build_test_array(&metadata, &["3,Charlie"]);
    writer.append_batch(&array3).expect("Failed to append third batch");
}

#[test]
fn test_vine_streaming_writer_flush() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let array = build_test_array(&metadata, &["1,Alice", "2,Bob"]);
    writer.append_batch(&array).expect("Failed to append batch");

    let result = writer.flush();
    assert!(result.is_ok());
}

#[test]
fn test_vine_streaming_writer_close() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let array = build_test_array(&metadata, &["1,Alice", "2,Bob"]);
    writer.append_batch(&array).expect("Failed to append batch");

    let result = writer.close();
    assert!(result.is_ok());
}

#[test]
fn test_vine_streaming_writer_write_and_read_roundtrip() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let array = build_test_array(&metadata, &["1,Alice", "2,Bob", "3,Charlie"]);
    writer.append_batch(&array).expect("Failed to append batch");
    writer.close().expect("Failed to close writer");

    // Read data back as VortexArrayRef
    let result = read_vine_data(base_path.to_str().unwrap());
    assert!(!result.is_empty());

    // Convert to Arrow and verify contents
    let mut total_rows = 0;
    for arr in &result {
        let batch = vortex_to_arrow(arr, true).expect("Failed to convert to Arrow");
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 2);
    }
    assert_eq!(total_rows, 3);
}

#[test]
fn test_vine_streaming_writer_flush_multiple_times() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");

    let array1 = build_test_array(&metadata, &["1,Alice"]);
    writer.append_batch(&array1).expect("Failed to append first batch");
    writer.flush().expect("Failed to flush first time");

    let array2 = build_test_array(&metadata, &["2,Bob"]);
    writer.append_batch(&array2).expect("Failed to append second batch");
    writer.flush().expect("Failed to flush second time");

    writer.close().expect("Failed to close writer");

    let result = read_vine_data(base_path.to_str().unwrap());
    let mut total_rows = 0;
    for arr in &result {
        let batch = vortex_to_arrow(arr, true).expect("Failed to convert");
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[test]
fn test_vine_streaming_writer_creates_date_partition() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = VineStreamingWriter::new(base_path).expect("Failed to create writer");
    let array = build_test_array(&metadata, &["1,Alice"]);
    writer.append_batch(&array).expect("Failed to append batch");
    writer.close().expect("Failed to close writer");

    let date_dirs: Vec<_> = fs::read_dir(base_path)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert!(!date_dirs.is_empty());

    let dir_name = date_dirs[0].file_name();
    let dir_name_str = dir_name.to_str().unwrap();
    assert!(dir_name_str.contains('-'));
    assert_eq!(dir_name_str.len(), 10);
}
