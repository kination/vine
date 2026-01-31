use vine_core::writer_cache::WriterCache;
use vine_core::metadata::{Metadata, MetadataField};
use tempfile::tempdir;
use std::path::PathBuf;

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
fn test_writer_cache_new() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata file
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create cache
    let cache = WriterCache::new(PathBuf::from(base_path)).expect("Failed to create cache");

    assert_eq!(cache.metadata.table_name, "test_table");
    assert_eq!(cache.metadata.fields.len(), 2);
    assert_eq!(cache.base_path, base_path);
}

#[test]
fn test_writer_cache_new_missing_file() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = PathBuf::from(temp_dir.path());

    let result = WriterCache::new(base_path);
    assert!(result.is_err());
}

#[test]
fn test_writer_cache_reload() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create initial metadata
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create cache
    let mut cache = WriterCache::new(PathBuf::from(base_path)).expect("Failed to create cache");
    assert_eq!(cache.metadata.table_name, "test_table");

    // Update metadata file
    let new_metadata = Metadata::new(
        "updated_table",
        vec![
            MetadataField {
                id: 1,
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_required: true,
            },
        ],
    );
    new_metadata.save(meta_path.to_str().unwrap()).expect("Failed to save updated metadata");

    // Reload cache
    cache.reload().expect("Failed to reload cache");

    assert_eq!(cache.metadata.table_name, "updated_table");
    assert_eq!(cache.metadata.fields.len(), 1);
}

#[test]
fn test_writer_cache_from_metadata() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = PathBuf::from(temp_dir.path());

    let metadata = create_test_metadata();
    let cache = WriterCache::from_metadata(base_path.clone(), metadata);

    assert_eq!(cache.metadata.table_name, "test_table");
    assert_eq!(cache.metadata.fields.len(), 2);
    assert_eq!(cache.base_path, base_path);
}

#[test]
fn test_writer_cache_from_metadata_no_file_needed() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = PathBuf::from(temp_dir.path());

    // Don't create any metadata file
    let metadata = create_test_metadata();
    let cache = WriterCache::from_metadata(base_path, metadata);

    // Should work without file
    assert_eq!(cache.metadata.table_name, "test_table");
}

#[test]
fn test_writer_cache_reload_after_file_deleted() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata file
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create cache
    let mut cache = WriterCache::new(PathBuf::from(base_path)).expect("Failed to create cache");

    // Delete metadata file
    std::fs::remove_file(&meta_path).expect("Failed to delete metadata");

    // Reload should fail
    let result = cache.reload();
    assert!(result.is_err());
}
