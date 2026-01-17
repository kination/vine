use vine_core::storage_reader::read_vine_data;
use vine_core::metadata::{Metadata, MetadataField};
use vine_core::vortex_exp::write_vortex_file;
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
fn test_read_vine_data_single_file() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create date directory
    let date_dir = base_path.join("2024-01-15");
    fs::create_dir(&date_dir).expect("Failed to create date dir");

    // Write test data
    let csv_rows = vec!["1,Alice".to_string(), "2,Bob".to_string()];
    let csv_rows_refs: Vec<&str> = csv_rows.iter().map(|s| s.as_str()).collect();
    let vtx_path = date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path, &metadata, &csv_rows_refs)
        .expect("Failed to write vortex file");

    // Read data
    let result = read_vine_data(base_path.to_str().unwrap());

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "1,Alice");
    assert_eq!(result[1], "2,Bob");
}

#[test]
fn test_read_vine_data_multiple_files() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create date directory
    let date_dir = base_path.join("2024-01-15");
    fs::create_dir(&date_dir).expect("Failed to create date dir");

    // Write first file
    let csv_rows1 = vec!["1,Alice".to_string()];
    let csv_rows1_refs: Vec<&str> = csv_rows1.iter().map(|s| s.as_str()).collect();
    let vtx_path1 = date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path1, &metadata, &csv_rows1_refs)
        .expect("Failed to write first vortex file");

    // Write second file
    let csv_rows2 = vec!["2,Bob".to_string()];
    let csv_rows2_refs: Vec<&str> = csv_rows2.iter().map(|s| s.as_str()).collect();
    let vtx_path2 = date_dir.join("data_130000_000000.vtx");
    write_vortex_file(&vtx_path2, &metadata, &csv_rows2_refs)
        .expect("Failed to write second vortex file");

    // Read data
    let result = read_vine_data(base_path.to_str().unwrap());

    assert_eq!(result.len(), 2);
}

#[test]
fn test_read_vine_data_multiple_dates() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create first date directory
    let date_dir1 = base_path.join("2024-01-14");
    fs::create_dir(&date_dir1).expect("Failed to create first date dir");
    let csv_rows1 = vec!["1,Alice".to_string()];
    let csv_rows1_refs: Vec<&str> = csv_rows1.iter().map(|s| s.as_str()).collect();
    let vtx_path1 = date_dir1.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path1, &metadata, &csv_rows1_refs)
        .expect("Failed to write first vortex file");

    // Create second date directory
    let date_dir2 = base_path.join("2024-01-15");
    fs::create_dir(&date_dir2).expect("Failed to create second date dir");
    let csv_rows2 = vec!["2,Bob".to_string()];
    let csv_rows2_refs: Vec<&str> = csv_rows2.iter().map(|s| s.as_str()).collect();
    let vtx_path2 = date_dir2.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path2, &metadata, &csv_rows2_refs)
        .expect("Failed to write second vortex file");

    // Read data (should be in chronological order)
    let result = read_vine_data(base_path.to_str().unwrap());

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "1,Alice"); // 2024-01-14 comes first
    assert_eq!(result[1], "2,Bob");   // 2024-01-15 comes second
}

#[test]
fn test_read_vine_data_empty_directory() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Read data from empty directory
    let result = read_vine_data(base_path.to_str().unwrap());

    assert!(result.is_empty());
}

#[test]
fn test_read_vine_data_missing_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Don't create metadata file
    let result = read_vine_data(base_path.to_str().unwrap());

    // Should return empty vector on error
    assert!(result.is_empty());
}

#[test]
fn test_read_vine_data_ignores_non_vtx_files() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create date directory
    let date_dir = base_path.join("2024-01-15");
    fs::create_dir(&date_dir).expect("Failed to create date dir");

    // Write vtx file
    let csv_rows = vec!["1,Alice".to_string()];
    let csv_rows_refs: Vec<&str> = csv_rows.iter().map(|s| s.as_str()).collect();
    let vtx_path = date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path, &metadata, &csv_rows_refs)
        .expect("Failed to write vortex file");

    // Create non-vtx file
    let txt_path = date_dir.join("README.txt");
    fs::write(&txt_path, "This should be ignored").expect("Failed to write txt file");

    // Read data
    let result = read_vine_data(base_path.to_str().unwrap());

    // Should only read the .vtx file
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "1,Alice");
}

#[test]
fn test_read_vine_data_ignores_invalid_date_directories() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create valid date directory
    let valid_date_dir = base_path.join("2024-01-15");
    fs::create_dir(&valid_date_dir).expect("Failed to create valid date dir");
    let csv_rows = vec!["1,Alice".to_string()];
    let csv_rows_refs: Vec<&str> = csv_rows.iter().map(|s| s.as_str()).collect();
    let vtx_path = valid_date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path, &metadata, &csv_rows_refs)
        .expect("Failed to write vortex file");

    // Create invalid date directory
    let invalid_date_dir = base_path.join("not-a-date");
    fs::create_dir(&invalid_date_dir).expect("Failed to create invalid date dir");

    // Read data
    let result = read_vine_data(base_path.to_str().unwrap());

    // Should only read from valid date directory
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "1,Alice");
}
