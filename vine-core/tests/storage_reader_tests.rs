use vine_core::storage_reader::read_vine_data;
use vine_core::metadata::{Metadata, MetadataField};
use vine_core::vortex_exp::write_vortex_file;
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

/// Helper: count total rows from read results using Arrow conversion
fn total_rows(arrays: &[vortex::ArrayRef]) -> usize {
    arrays.iter().map(|a| {
        vortex_to_arrow(a, true).expect("Failed to convert").num_rows()
    }).sum()
}

#[test]
fn test_read_vine_data_single_file() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let date_dir = base_path.join("2024-01-15");
    fs::create_dir(&date_dir).expect("Failed to create date dir");

    let vtx_path = date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path, &metadata, &["1,Alice", "2,Bob"])
        .expect("Failed to write vortex file");

    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(result.len(), 1); // 1 file = 1 array
    assert_eq!(total_rows(&result), 2);

    // Verify column structure
    let batch = vortex_to_arrow(&result[0], true).expect("Failed to convert");
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn test_read_vine_data_multiple_files() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let date_dir = base_path.join("2024-01-15");
    fs::create_dir(&date_dir).expect("Failed to create date dir");

    let vtx_path1 = date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path1, &metadata, &["1,Alice"])
        .expect("Failed to write first vortex file");

    let vtx_path2 = date_dir.join("data_130000_000000.vtx");
    write_vortex_file(&vtx_path2, &metadata, &["2,Bob"])
        .expect("Failed to write second vortex file");

    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(total_rows(&result), 2);
}

#[test]
fn test_read_vine_data_multiple_dates() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let date_dir1 = base_path.join("2024-01-14");
    fs::create_dir(&date_dir1).expect("Failed to create first date dir");
    let vtx_path1 = date_dir1.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path1, &metadata, &["1,Alice"])
        .expect("Failed to write first vortex file");

    let date_dir2 = base_path.join("2024-01-15");
    fs::create_dir(&date_dir2).expect("Failed to create second date dir");
    let vtx_path2 = date_dir2.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path2, &metadata, &["2,Bob"])
        .expect("Failed to write second vortex file");

    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(total_rows(&result), 2);
}

#[test]
fn test_read_vine_data_empty_directory() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let result = read_vine_data(base_path.to_str().unwrap());
    assert!(result.is_empty());
}

#[test]
fn test_read_vine_data_missing_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let result = read_vine_data(base_path.to_str().unwrap());
    assert!(result.is_empty());
}

#[test]
fn test_read_vine_data_ignores_non_vtx_files() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let date_dir = base_path.join("2024-01-15");
    fs::create_dir(&date_dir).expect("Failed to create date dir");

    let vtx_path = date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path, &metadata, &["1,Alice"])
        .expect("Failed to write vortex file");

    let txt_path = date_dir.join("README.txt");
    fs::write(&txt_path, "This should be ignored").expect("Failed to write txt file");

    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn test_read_vine_data_ignores_invalid_date_directories() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let valid_date_dir = base_path.join("2024-01-15");
    fs::create_dir(&valid_date_dir).expect("Failed to create valid date dir");
    let vtx_path = valid_date_dir.join("data_120000_000000.vtx");
    write_vortex_file(&vtx_path, &metadata, &["1,Alice"])
        .expect("Failed to write vortex file");

    let invalid_date_dir = base_path.join("not-a-date");
    fs::create_dir(&invalid_date_dir).expect("Failed to create invalid date dir");

    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(total_rows(&result), 1);
}
