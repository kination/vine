use vine_core::reader_cache::ReaderCache;
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
fn test_reader_cache_new() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata file
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create cache
    let cache = ReaderCache::new(PathBuf::from(base_path)).expect("Failed to create cache");

    assert_eq!(cache.metadata.table_name, "test_table");
    assert_eq!(cache.metadata.fields.len(), 2);
    assert_eq!(cache.base_path, base_path);
}

#[test]
fn test_reader_cache_new_missing_file() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = PathBuf::from(temp_dir.path());

    let result = ReaderCache::new(base_path);
    assert!(result.is_err());
}

#[test]
fn test_reader_cache_new_empty_fields() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata with no fields
    let metadata = Metadata::new("empty_table", vec![]);
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Should fail because fields are empty
    let result = ReaderCache::new(PathBuf::from(base_path));
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.to_string().contains("at least one field"));
    }
}

#[test]
fn test_reader_cache_field_count() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create metadata file
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create cache
    let cache = ReaderCache::new(PathBuf::from(base_path)).expect("Failed to create cache");

    assert_eq!(cache.field_count(), 2);
}

#[test]
fn test_reader_cache_new_with_fallback_vine_meta() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create vine_meta.json (Option 1)
    let metadata = create_test_metadata();
    let meta_path = base_path.join("vine_meta.json");
    metadata.save(meta_path.to_str().unwrap()).expect("Failed to save metadata");

    // Create cache using fallback
    let cache = ReaderCache::new_with_fallback(PathBuf::from(base_path))
        .expect("Failed to create cache");

    assert_eq!(cache.metadata.table_name, "test_table");
    assert_eq!(cache.field_count(), 2);
}

#[test]
fn test_reader_cache_new_with_fallback_cached_schema() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create cached schema (Option 2)
    let metadata = create_test_metadata();
    metadata.save_to_cache(base_path).expect("Failed to save to cache");

    // Create cache using fallback (should use cached schema)
    let cache = ReaderCache::new_with_fallback(PathBuf::from(base_path))
        .expect("Failed to create cache");

    assert_eq!(cache.metadata.table_name, "test_table");
    assert_eq!(cache.field_count(), 2);
}

#[test]
fn test_reader_cache_new_with_fallback_cached_empty_fields() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    // Create cached schema with empty fields
    let metadata = Metadata::new("empty_table", vec![]);
    metadata.save_to_cache(base_path).expect("Failed to save to cache");

    // Should fail because cached metadata has empty fields
    let result = ReaderCache::new_with_fallback(PathBuf::from(base_path));
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.to_string().contains("at least one field"));
    }
}
