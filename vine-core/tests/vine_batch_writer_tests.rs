use vine_core::vine_batch_writer::VineBatchWriter;
use vine_core::metadata::{Metadata, MetadataField};
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

/// Helper: build a VortexArrayRef from comma-separated rows using metadata
fn build_test_array(metadata: &Metadata, rows: &[&str]) -> vortex::ArrayRef {
    build_struct_array(metadata, rows).expect("Failed to build test array")
}

#[test]
fn test_vine_batch_writer_write() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let array = build_test_array(&metadata, &["1,Alice", "2,Bob", "3,Charlie"]);
    VineBatchWriter::write(base_path, &array).expect("Failed to write data");

    // Verify data was written and can be read back
    let result = read_vine_data(base_path.to_str().unwrap());
    assert_eq!(result.len(), 1); // 1 array (1 file)

    // Convert back to Arrow to verify contents
    let batch = vortex_to_arrow(&result[0], true).expect("Failed to convert to Arrow");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn test_vine_batch_writer_write_without_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // With direct array writes, metadata file is NOT required on disk
    // because the array already carries its schema
    let metadata = create_test_metadata();
    let array = build_test_array(&metadata, &["1,Alice"]);
    let result = VineBatchWriter::write(base_path, &array);
    assert!(result.is_ok(), "Direct array write should succeed without metadata file");
}

#[test]
fn test_vine_batch_writer_creates_date_partition() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let array = build_test_array(&metadata, &["1,Alice"]);
    VineBatchWriter::write(base_path, &array).expect("Failed to write data");

    // Verify date partition directory was created
    let date_dirs: Vec<_> = fs::read_dir(base_path)
        .expect("Failed to read dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    assert_eq!(date_dirs.len(), 1);

    let dir_name = date_dirs[0].file_name();
    let dir_name_str = dir_name.to_str().unwrap();
    assert!(dir_name_str.contains('-'));
    assert_eq!(dir_name_str.len(), 10); // YYYY-MM-DD
}

#[test]
fn test_vine_batch_writer_creates_vtx_file() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let array = build_test_array(&metadata, &["1,Alice"]);
    VineBatchWriter::write(base_path, &array).expect("Failed to write data");

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

    let file_name = vtx_files[0].file_name();
    let file_name_str = file_name.to_str().unwrap();
    assert!(file_name_str.starts_with("data_"));
    assert!(file_name_str.ends_with(".vtx"));
}

#[test]
fn test_vine_batch_writer_multiple_writes() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    let array1 = build_test_array(&metadata, &["1,Alice"]);
    VineBatchWriter::write(base_path, &array1).expect("Failed to write first batch");

    let array2 = build_test_array(&metadata, &["2,Bob"]);
    VineBatchWriter::write(base_path, &array2).expect("Failed to write second batch");

    let result = read_vine_data(base_path.to_str().unwrap());
    // Each write creates a separate file, so 2 arrays
    assert_eq!(result.len(), 2);
}
