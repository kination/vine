use vine_core::streaming_writer_v2::StreamingWriterV2;
use vine_core::writer_config::WriterConfig;
use vine_core::metadata::{Metadata, MetadataField};
use vine_core::vortex_exp::build_struct_array;
use tempfile::tempdir;
use chrono::Local;

fn create_test_metadata() -> Metadata {
    Metadata::new(
        "test_stream_v2",
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
fn test_streaming_writer_v2_basic() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let path = temp_dir.path();

    let meta_path = path.join("vine_meta.json");
    let metadata = create_test_metadata();
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = StreamingWriterV2::new(path.to_path_buf())
        .expect("Failed to create writer");

    // Write and accumulate
    let array1 = build_test_array(&metadata, &["1,Alice", "2,Bob"]);
    writer.write_batch(&array1).expect("Write failed");
    assert_eq!(writer.buffered_rows(), 2);
    assert_eq!(writer.buffered_chunks(), 1);

    let array2 = build_test_array(&metadata, &["3,Charlie"]);
    writer.write_batch(&array2).expect("Write failed");
    assert_eq!(writer.buffered_rows(), 3);
    assert_eq!(writer.buffered_chunks(), 2);

    // Flush
    let summary = writer.flush().expect("Flush failed");
    assert!(summary.is_some(), "Should return flush summary");
    let summary = summary.unwrap();

    assert_eq!(summary.rows_written, 3, "Should have written 3 rows");
    assert!(summary.bytes_written > 0, "Should have written bytes");
    assert!(summary.file_path.exists(), "File should exist");

    assert_eq!(writer.buffered_rows(), 0);
    assert_eq!(writer.buffered_chunks(), 0);
    assert!(writer.bytes_written() > 0);

    // Write more (new file)
    let array3 = build_test_array(&metadata, &["4,Diana"]);
    writer.write_batch(&array3).expect("Write failed");
    writer.close().expect("Close failed");

    // Verify files
    let date_str = Local::now().format("%Y-%m-%d").to_string();
    let partition_dir = path.join(&date_str);
    assert!(partition_dir.exists());

    let files: Vec<_> = std::fs::read_dir(&partition_dir)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "vtx"))
        .collect();

    assert!(files.len() >= 2, "Should create at least 2 files");
}

#[test]
fn test_auto_flush() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let path = temp_dir.path();

    let meta_path = path.join("vine_meta.json");
    let metadata = create_test_metadata();
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut config = WriterConfig::default();
    config.max_rows_per_file = 5;

    let mut writer = StreamingWriterV2::with_config(path.to_path_buf(), config)
        .expect("Failed to create writer");

    let array1 = build_test_array(&metadata, &["1,A", "2,B", "3,C"]);
    writer.write_batch(&array1).expect("Write failed");
    assert_eq!(writer.buffered_rows(), 3);

    let array2 = build_test_array(&metadata, &["4,D", "5,E", "6,F"]);
    writer.write_batch(&array2).expect("Write failed");
    assert_eq!(writer.buffered_rows(), 3);

    writer.close().expect("Close failed");
}

#[test]
fn test_empty_flush() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let path = temp_dir.path();

    let meta_path = path.join("vine_meta.json");
    let metadata = create_test_metadata();
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let mut writer = StreamingWriterV2::new(path.to_path_buf())
        .expect("Failed to create writer");

    let summary = writer.flush().expect("Flush should succeed");
    assert!(summary.is_none(), "Empty flush should return None");
    assert_eq!(writer.bytes_written(), 0);

    writer.close().expect("Close failed");
}
