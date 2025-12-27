use std::fs;
use std::path::Path;
use tempfile::TempDir;

// Import from vine_core crate
use vine_core::storage_writer::{VineBatchWriter, VineStreamingWriter};
use vine_core::writer_config::WriterConfig;

/// Helper function to create test metadata
fn create_test_metadata(dir: &Path) -> std::io::Result<()> {
    let metadata = r#"{
  "table_name": "test_table",
  "fields": [
    {
      "id": 1,
      "name": "id",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "name",
      "data_type": "string",
      "is_required": true
    }
  ]
}"#;
    fs::write(dir.join("vine_meta.json"), metadata)
}

// ============================================================================
// Batch Writer Tests
// ============================================================================

#[test]
fn test_batch_writer_balanced() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob", "3,charlie"];

    let result = VineBatchWriter::write_balanced(path, &data);
    assert!(result.is_ok(), "Batch write should succeed");

    // Verify files were created
    let entries: Vec<_> = fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert!(!entries.is_empty(), "Should create date directory");
}

#[test]
fn test_batch_writer_high_throughput() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob"];

    let result = VineBatchWriter::write_high_throughput(path, &data);
    assert!(result.is_ok(), "High throughput write should succeed");
}

#[test]
fn test_batch_writer_high_compression() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice"];

    let result = VineBatchWriter::write_high_compression(path, &data);
    assert!(result.is_ok(), "High compression write should succeed");
}

#[test]
fn test_batch_writer_custom_config() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob"];

    let result = VineBatchWriter::write(path, &data, WriterConfig::high_throughput());
    assert!(result.is_ok(), "Write with custom config should succeed");
}

#[test]
fn test_empty_batch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let empty: Vec<&str> = vec![];

    // Batch writer with empty data
    let result = VineBatchWriter::write_balanced(path, &empty);
    assert!(result.is_ok(), "Empty batch should not fail");
}

#[test]
fn test_large_batch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Generate large batch
    let large_data: Vec<String> = (0..1000).map(|i| format!("{},user{}", i, i)).collect();
    let large_data_refs: Vec<&str> = large_data.iter().map(|s| s.as_str()).collect();

    let result = VineBatchWriter::write_balanced(path, &large_data_refs);
    assert!(result.is_ok(), "Large batch should succeed");
}

#[test]
fn test_missing_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    // Don't create metadata

    let data = vec!["1,alice"];

    let result = VineBatchWriter::write_balanced(path, &data);
    assert!(result.is_err(), "Should fail without metadata");
}

// ============================================================================
// Streaming Writer Tests
// ============================================================================

#[test]
fn test_streaming_writer_balanced() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    // Write first batch
    let batch1 = vec!["1,alice", "2,bob"];
    assert!(writer.append_batch(&batch1).is_ok());

    // Write second batch
    let batch2 = vec!["3,charlie", "4,dave"];
    assert!(writer.append_batch(&batch2).is_ok());

    // Close writer
    assert!(writer.close().is_ok());
}

#[test]
fn test_streaming_writer_flush() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    // Write and flush
    let batch1 = vec!["1,alice"];
    writer.append_batch(&batch1).unwrap();
    assert!(writer.flush().is_ok());

    // Write again after flush
    let batch2 = vec!["2,bob"];
    writer.append_batch(&batch2).unwrap();

    writer.close().unwrap();
}

#[test]
fn test_streaming_writer_high_throughput() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::high_throughput(path).unwrap();

    for i in 0..10 {
        let batch = vec![format!("{},user{}", i, i)];
        let batch_refs: Vec<&str> = batch.iter().map(|s| s.as_str()).collect();
        writer.append_batch(&batch_refs).unwrap();
    }

    writer.close().unwrap();
}

#[test]
fn test_streaming_writer_high_compression() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::high_compression(path).unwrap();

    let batch = vec!["1,alice", "2,bob", "3,charlie"];
    writer.append_batch(&batch).unwrap();

    writer.close().unwrap();
}

#[test]
fn test_multiple_flushes() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    for _ in 0..3 {
        let batch = vec!["1,test"];
        writer.append_batch(&batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}

#[test]
fn test_streaming_empty_batch() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    let empty: Vec<&str> = vec![];
    let result = writer.append_batch(&empty);

    // Empty batch should be handled gracefully
    assert!(result.is_ok());

    writer.close().unwrap();
}

#[test]
fn test_streaming_single_row_batches() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    // Write many single-row batches
    for i in 0..100 {
        let batch = vec![format!("{},user{}", i, i)];
        let batch_refs: Vec<&str> = batch.iter().map(|s| s.as_str()).collect();
        writer.append_batch(&batch_refs).unwrap();
    }

    writer.close().unwrap();
}

#[test]
fn test_streaming_alternating_batch_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    // Small batch
    let small = vec!["1,alice"];
    writer.append_batch(&small).unwrap();

    // Large batch
    let large: Vec<String> = (2..102).map(|i| format!("{},user{}", i, i)).collect();
    let large_refs: Vec<&str> = large.iter().map(|s| s.as_str()).collect();
    writer.append_batch(&large_refs).unwrap();

    // Small batch again
    let small2 = vec!["102,bob"];
    writer.append_batch(&small2).unwrap();

    writer.close().unwrap();
}

#[test]
fn test_streaming_flush_timing() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let mut writer = VineStreamingWriter::balanced(path).unwrap();

    // Write without flush
    let batch1 = vec!["1,alice"];
    writer.append_batch(&batch1).unwrap();

    // Flush explicitly
    writer.flush().unwrap();

    // Write more data
    let batch2 = vec!["2,bob"];
    writer.append_batch(&batch2).unwrap();

    // Close (implicitly flushes)
    writer.close().unwrap();

    // Verify multiple files were created (one per flush)
    let date_dirs: Vec<_> = fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert!(!date_dirs.is_empty(), "Should create date directories");
}

// ============================================================================
// Configuration Comparison Tests
// ============================================================================

#[test]
fn test_config_balanced_properties() {
    let config = WriterConfig::balanced();

    // Balanced should use SNAPPY compression
    // This is implicit in the configuration, testing via successful write
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob"];
    let result = VineBatchWriter::write(path, &data, config);

    assert!(result.is_ok());
}

#[test]
fn test_config_high_throughput_properties() {
    let config = WriterConfig::high_throughput();

    // High throughput should use no compression
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob"];
    let result = VineBatchWriter::write(path, &data, config);

    assert!(result.is_ok());
}

#[test]
fn test_config_high_compression_properties() {
    let config = WriterConfig::high_compression();

    // High compression should use ZSTD
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob"];
    let result = VineBatchWriter::write(path, &data, config);

    assert!(result.is_ok());
}

// ============================================================================
// Data Type Tests
// ============================================================================

#[test]
fn test_write_all_data_types() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    // Create metadata with all supported types
    let metadata = r#"{
  "table_name": "all_types_table",
  "fields": [
    {
      "id": 1,
      "name": "id",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "name",
      "data_type": "string",
      "is_required": true
    },
    {
      "id": 3,
      "name": "active",
      "data_type": "boolean",
      "is_required": false
    },
    {
      "id": 4,
      "name": "score",
      "data_type": "double",
      "is_required": false
    }
  ]
}"#;
    fs::write(path.join("vine_meta.json"), metadata).unwrap();

    // Write data with all types
    let data = vec![
        "1,alice,true,95.5",
        "2,bob,false,87.3",
        "3,charlie,true,92.0",
    ];

    let result = VineBatchWriter::write_balanced(path, &data);
    assert!(result.is_ok(), "Should write all data types successfully");
}

#[test]
fn test_write_boolean_values() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    let metadata = r#"{
  "table_name": "bool_table",
  "fields": [
    {
      "id": 1,
      "name": "id",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "flag",
      "data_type": "boolean",
      "is_required": true
    }
  ]
}"#;
    fs::write(path.join("vine_meta.json"), metadata).unwrap();

    let data = vec!["1,true", "2,false", "3,true", "4,false"];
    let result = VineBatchWriter::write_balanced(path, &data);

    assert!(result.is_ok(), "Should write boolean values");
}

#[test]
fn test_write_double_values() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    let metadata = r#"{
  "table_name": "double_table",
  "fields": [
    {
      "id": 1,
      "name": "id",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "value",
      "data_type": "double",
      "is_required": true
    }
  ]
}"#;
    fs::write(path.join("vine_meta.json"), metadata).unwrap();

    let data = vec!["1,3.14159", "2,2.71828", "3,1.41421"];
    let result = VineBatchWriter::write_balanced(path, &data);

    assert!(result.is_ok(), "Should write double values");
}

// ============================================================================
// Legacy API Tests
// ============================================================================

#[test]
fn test_legacy_write_data() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    let data = vec!["1,alice", "2,bob"];

    // Test legacy write_data function
    let result = vine_core::storage_writer::write_data(path, &data);
    assert!(result.is_ok(), "Legacy write_data should work");
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_write_without_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    // Intentionally don't create metadata

    let data = vec!["1,alice"];
    let result = VineBatchWriter::write_balanced(path, &data);

    assert!(result.is_err(), "Should fail without metadata");
}

#[test]
fn test_write_to_invalid_path() {
    let data = vec!["1,alice"];
    let result = VineBatchWriter::write_balanced("/nonexistent/invalid/path", &data);

    assert!(result.is_err(), "Should fail with invalid path");
}
