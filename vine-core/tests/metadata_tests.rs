use vine_core::metadata::{Metadata, MetadataField, Value};
use tempfile::tempdir;

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
fn test_metadata_new() {
    let metadata = create_test_metadata();

    assert_eq!(metadata.table_name, "test_table");
    assert_eq!(metadata.fields.len(), 2);
    assert_eq!(metadata.fields[0].name, "id");
    assert_eq!(metadata.fields[1].name, "name");
}

#[test]
fn test_metadata_empty_fields() {
    let metadata = Metadata::new("empty_table", vec![]);

    assert_eq!(metadata.table_name, "empty_table");
    assert!(metadata.fields.is_empty());
}

#[test]
fn test_metadata_save_and_load() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let meta_path = temp_dir.path().join("vine_meta.json");
    let meta_path_str = meta_path.to_str().unwrap();

    let original = create_test_metadata();
    original.save(meta_path_str).expect("Failed to save metadata");

    let loaded = Metadata::load(&meta_path).expect("Failed to load metadata");

    assert_eq!(loaded.table_name, original.table_name);
    assert_eq!(loaded.fields.len(), original.fields.len());

    for (orig, loaded) in original.fields.iter().zip(loaded.fields.iter()) {
        assert_eq!(orig.id, loaded.id);
        assert_eq!(orig.name, loaded.name);
        assert_eq!(orig.data_type, loaded.data_type);
        assert_eq!(orig.is_required, loaded.is_required);
    }
}

#[test]
fn test_metadata_load_nonexistent_file() {
    let result = Metadata::load("/nonexistent/path/vine_meta.json");
    assert!(result.is_err());
}

#[test]
fn test_metadata_save_to_cache_and_load_cached() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let base_path = temp_dir.path();

    let metadata = create_test_metadata();
    metadata.save_to_cache(base_path).expect("Failed to save to cache");

    // Verify cache directory was created
    let cache_dir = base_path.join("_meta");
    assert!(cache_dir.exists());

    // Load from cache
    let loaded = Metadata::load_cached(base_path);
    assert!(loaded.is_some());

    let loaded = loaded.unwrap();
    assert_eq!(loaded.table_name, metadata.table_name);
    assert_eq!(loaded.fields.len(), metadata.fields.len());
}

#[test]
fn test_metadata_load_cached_nonexistent() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let result = Metadata::load_cached(temp_dir.path());
    assert!(result.is_none());
}

#[test]
fn test_metadata_field_types() {
    let metadata = Metadata::new(
        "all_types",
        vec![
            MetadataField { id: 1, name: "byte_col".to_string(), data_type: "byte".to_string(), is_required: true },
            MetadataField { id: 2, name: "short_col".to_string(), data_type: "short".to_string(), is_required: true },
            MetadataField { id: 3, name: "int_col".to_string(), data_type: "integer".to_string(), is_required: true },
            MetadataField { id: 4, name: "long_col".to_string(), data_type: "long".to_string(), is_required: true },
            MetadataField { id: 5, name: "float_col".to_string(), data_type: "float".to_string(), is_required: true },
            MetadataField { id: 6, name: "double_col".to_string(), data_type: "double".to_string(), is_required: true },
            MetadataField { id: 7, name: "bool_col".to_string(), data_type: "boolean".to_string(), is_required: true },
            MetadataField { id: 8, name: "str_col".to_string(), data_type: "string".to_string(), is_required: true },
            MetadataField { id: 9, name: "date_col".to_string(), data_type: "date".to_string(), is_required: false },
            MetadataField { id: 10, name: "ts_col".to_string(), data_type: "timestamp".to_string(), is_required: false },
        ],
    );

    assert_eq!(metadata.fields.len(), 10);
    assert_eq!(metadata.fields[0].data_type, "byte");
    assert_eq!(metadata.fields[9].data_type, "timestamp");
}

#[test]
fn test_value_enum_variants() {
    // Test all Value enum variants can be created
    let byte_val = Value::Byte(127);
    let short_val = Value::Short(32767);
    let int_val = Value::Int(2147483647);
    let long_val = Value::Long(9223372036854775807);
    let float_val = Value::Float(3.14);
    let double_val = Value::Double(2.718281828);
    let bool_val = Value::Bool(true);
    let string_val = Value::String("hello".to_string());
    let binary_val = Value::Binary(vec![0x01, 0x02, 0x03]);
    let date_val = Value::Date(19723); // Days since epoch
    let timestamp_val = Value::Timestamp(1704067200000); // Millis since epoch
    let decimal_val = Value::Decimal("123.456".to_string());

    // Verify values using pattern matching
    match byte_val {
        Value::Byte(v) => assert_eq!(v, 127),
        _ => panic!("Expected Byte"),
    }
    match short_val {
        Value::Short(v) => assert_eq!(v, 32767),
        _ => panic!("Expected Short"),
    }
    match int_val {
        Value::Int(v) => assert_eq!(v, 2147483647),
        _ => panic!("Expected Int"),
    }
    match long_val {
        Value::Long(v) => assert_eq!(v, 9223372036854775807),
        _ => panic!("Expected Long"),
    }
    match float_val {
        Value::Float(v) => assert!((v - 3.14).abs() < 0.001),
        _ => panic!("Expected Float"),
    }
    match double_val {
        Value::Double(v) => assert!((v - 2.718281828).abs() < 0.000001),
        _ => panic!("Expected Double"),
    }
    match bool_val {
        Value::Bool(v) => assert!(v),
        _ => panic!("Expected Bool"),
    }
    match string_val {
        Value::String(v) => assert_eq!(v, "hello"),
        _ => panic!("Expected String"),
    }
    match binary_val {
        Value::Binary(v) => assert_eq!(v, vec![0x01, 0x02, 0x03]),
        _ => panic!("Expected Binary"),
    }
    match date_val {
        Value::Date(v) => assert_eq!(v, 19723),
        _ => panic!("Expected Date"),
    }
    match timestamp_val {
        Value::Timestamp(v) => assert_eq!(v, 1704067200000),
        _ => panic!("Expected Timestamp"),
    }
    match decimal_val {
        Value::Decimal(v) => assert_eq!(v, "123.456"),
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_metadata_clone() {
    let original = create_test_metadata();
    let cloned = original.clone();

    assert_eq!(original.table_name, cloned.table_name);
    assert_eq!(original.fields.len(), cloned.fields.len());
}

#[test]
fn test_metadata_field_clone() {
    let field = MetadataField {
        id: 1,
        name: "test".to_string(),
        data_type: "integer".to_string(),
        is_required: true,
    };

    let cloned = field.clone();

    assert_eq!(field.id, cloned.id);
    assert_eq!(field.name, cloned.name);
    assert_eq!(field.data_type, cloned.data_type);
    assert_eq!(field.is_required, cloned.is_required);
}
