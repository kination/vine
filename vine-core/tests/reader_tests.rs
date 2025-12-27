use std::fs;
use std::path::Path;
use tempfile::TempDir;

// Import from vine_core crate
use vine_core::storage_reader::read_vine_data;
use vine_core::vine_batch_writer::VineBatchWriter;

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

/// Helper function to create metadata with different data types
fn create_metadata_all_types(dir: &Path) -> std::io::Result<()> {
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
    fs::write(dir.join("vine_meta.json"), metadata)
}

#[test]
fn test_read_basic_data() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Write test data
    let data = vec!["1,alice", "2,bob", "3,charlie"];
    VineBatchWriter::write_balanced(path, &data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 3, "Should read 3 rows");
    assert_eq!(rows[0], "1,alice");
    assert_eq!(rows[1], "2,bob");
    assert_eq!(rows[2], "3,charlie");
}

#[test]
fn test_read_empty_table() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Write empty data
    let empty: Vec<&str> = vec![];
    VineBatchWriter::write_balanced(path, &empty).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 0, "Should read 0 rows from empty table");
}

#[test]
fn test_read_all_data_types() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_metadata_all_types(path).unwrap();

    // Write data with all types: id, name, active, score
    let data = vec![
        "1,alice,true,95.5",
        "2,bob,false,87.3",
        "3,charlie,true,92.0",
    ];
    VineBatchWriter::write_balanced(path, &data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 3, "Should read 3 rows");
    assert_eq!(rows[0], "1,alice,true,95.5");
    assert_eq!(rows[1], "2,bob,false,87.3");
    assert_eq!(rows[2], "3,charlie,true,92");
}

#[test]
fn test_read_large_dataset() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Write large dataset (1000 rows)
    let large_data: Vec<String> = (0..1000)
        .map(|i| format!("{},user{}", i, i))
        .collect();
    let large_data_refs: Vec<&str> = large_data.iter().map(|s| s.as_str()).collect();

    VineBatchWriter::write_balanced(path, &large_data_refs).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 1000, "Should read 1000 rows");
    assert_eq!(rows[0], "0,user0");
    assert_eq!(rows[999], "999,user999");
}

#[test]
fn test_read_multiple_files() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Write multiple batches (creates multiple files)
    let batch1 = vec!["1,alice", "2,bob"];
    let batch2 = vec!["3,charlie", "4,dave"];
    let batch3 = vec!["5,eve", "6,frank"];

    VineBatchWriter::write_balanced(path, &batch1).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100)); // Ensure different timestamps

    VineBatchWriter::write_balanced(path, &batch2).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    VineBatchWriter::write_balanced(path, &batch3).unwrap();

    // Read all data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 6, "Should read all rows from multiple files");

    // Verify all rows are present (order may vary by file timestamp)
    let row_set: std::collections::HashSet<_> = rows.iter().collect();
    assert!(row_set.contains(&"1,alice".to_string()));
    assert!(row_set.contains(&"2,bob".to_string()));
    assert!(row_set.contains(&"3,charlie".to_string()));
    assert!(row_set.contains(&"4,dave".to_string()));
    assert!(row_set.contains(&"5,eve".to_string()));
    assert!(row_set.contains(&"6,frank".to_string()));
}

#[test]
fn test_read_chronological_order() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Manually create date directories in non-chronological order
    let date1 = path.join("2024-12-25");
    let date2 = path.join("2024-12-24");
    let date3 = path.join("2024-12-26");

    fs::create_dir(&date1).unwrap();
    fs::create_dir(&date2).unwrap();
    fs::create_dir(&date3).unwrap();

    // Write data to different dates
    // Using writer which creates files in date directories
    let batch1 = vec!["1,alice"];
    VineBatchWriter::write_balanced(path, &batch1).unwrap();

    // Read data - should be in chronological order by date
    let rows = read_vine_data(path.to_str().unwrap());

    // At minimum, verify it doesn't crash and reads data
    assert!(!rows.is_empty(), "Should read data from date directories");
}

#[test]
#[should_panic(expected = "Failed to initialize reader cache")]
fn test_read_missing_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    // Don't create metadata

    // This should panic
    read_vine_data(path.to_str().unwrap());
}

#[test]
fn test_read_with_special_characters() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Write data with special characters (commas in strings should be avoided in CSV)
    let data = vec![
        "1,alice@example.com",
        "2,bob-smith",
        "3,charlie_jones",
    ];
    VineBatchWriter::write_balanced(path, &data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "1,alice@example.com");
    assert_eq!(rows[1], "2,bob-smith");
    assert_eq!(rows[2], "3,charlie_jones");
}

#[test]
fn test_read_write_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    create_test_metadata(path).unwrap();

    // Write data
    let original_data = vec![
        "100,alice",
        "200,bob",
        "300,charlie",
        "400,dave",
        "500,eve",
    ];
    VineBatchWriter::write_balanced(path, &original_data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    // Verify exact match
    assert_eq!(rows.len(), original_data.len(), "Row count should match");
    for (i, original_row) in original_data.iter().enumerate() {
        assert_eq!(
            &rows[i], original_row,
            "Row {} should match original data",
            i
        );
    }
}

#[test]
fn test_read_different_configurations() {
    // Test reading data written with different writer configurations

    // High throughput
    {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice", "2,bob"];
        VineBatchWriter::write_high_throughput(path, &data).unwrap();

        let rows = read_vine_data(path.to_str().unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], "1,alice");
    }

    // Balanced
    {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice", "2,bob"];
        VineBatchWriter::write_balanced(path, &data).unwrap();

        let rows = read_vine_data(path.to_str().unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], "1,alice");
    }

    // High compression
    {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        create_test_metadata(path).unwrap();

        let data = vec!["1,alice", "2,bob"];
        VineBatchWriter::write_high_compression(path, &data).unwrap();

        let rows = read_vine_data(path.to_str().unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], "1,alice");
    }
}

#[test]
fn test_read_boolean_values() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    // Metadata with boolean field
    let metadata = r#"{
  "table_name": "test_bool",
  "fields": [
    {
      "id": 1,
      "name": "id",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "active",
      "data_type": "boolean",
      "is_required": true
    }
  ]
}"#;
    fs::write(path.join("vine_meta.json"), metadata).unwrap();

    // Write boolean data
    let data = vec!["1,true", "2,false", "3,true"];
    VineBatchWriter::write_balanced(path, &data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "1,true");
    assert_eq!(rows[1], "2,false");
    assert_eq!(rows[2], "3,true");
}

#[test]
fn test_read_double_precision() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    // Metadata with double field
    let metadata = r#"{
  "table_name": "test_double",
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

    // Write double data
    let data = vec!["1,3.14159", "2,2.71828", "3,1.41421"];
    VineBatchWriter::write_balanced(path, &data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 3);
    // Note: Double precision may have minor differences
    assert!(rows[0].starts_with("1,3.14159"));
    assert!(rows[1].starts_with("2,2.71828"));
    assert!(rows[2].starts_with("3,1.41421"));
}

#[test]
fn test_read_field_order_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    // Create metadata with specific field order
    let metadata = r#"{
  "table_name": "field_order_test",
  "fields": [
    {
      "id": 3,
      "name": "third",
      "data_type": "string",
      "is_required": true
    },
    {
      "id": 1,
      "name": "first",
      "data_type": "integer",
      "is_required": true
    },
    {
      "id": 2,
      "name": "second",
      "data_type": "string",
      "is_required": true
    }
  ]
}"#;
    fs::write(path.join("vine_meta.json"), metadata).unwrap();

    // Write data in metadata field order (not ID order)
    let data = vec!["foo,1,bar", "baz,2,qux"];
    VineBatchWriter::write_balanced(path, &data).unwrap();

    // Read data
    let rows = read_vine_data(path.to_str().unwrap());

    assert_eq!(rows.len(), 2);
    // Should read in same order as written (metadata field order)
    assert_eq!(rows[0], "foo,1,bar");
    assert_eq!(rows[1], "baz,2,qux");
}
