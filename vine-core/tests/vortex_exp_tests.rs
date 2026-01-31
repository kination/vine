use vine_core::vortex_exp::{
    build_struct_array, dtype_to_metadata, get_field_dtype_by_index, is_compatible_dtype,
    metadata_to_dtype, parse_date_to_days, parse_timestamp_to_millis, read_vortex_file_async,
    vortex_version, write_vortex_file_async,
};
use vine_core::metadata::{Metadata, MetadataField};
use vortex_dtype::{DType, Nullability, PType};

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
            MetadataField {
                id: 3,
                name: "active".to_string(),
                data_type: "boolean".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 4,
                name: "score".to_string(),
                data_type: "double".to_string(),
                is_required: false,
            },
        ],
    )
}

#[test]
fn test_metadata_to_dtype_conversion() {
    let metadata = create_test_metadata();
    let dtype = metadata_to_dtype(&metadata).expect("Should convert metadata to dtype");

    match &dtype {
        DType::Struct(struct_fields, _) => {
            assert_eq!(struct_fields.names().len(), 4);
            assert_eq!(struct_fields.names()[0].as_ref(), "id");
            assert_eq!(struct_fields.names()[1].as_ref(), "name");
            assert_eq!(struct_fields.names()[2].as_ref(), "active");
            assert_eq!(struct_fields.names()[3].as_ref(), "score");
        }
        _ => panic!("Expected Struct DType"),
    }

    println!("[TEST] DType conversion successful: {:?}", dtype);
}

#[test]
fn test_dtype_to_metadata_roundtrip() {
    let original = create_test_metadata();
    let dtype = metadata_to_dtype(&original).expect("Should convert to dtype");
    let converted = dtype_to_metadata(&dtype, "roundtrip_table")
        .expect("Should convert back to metadata");

    assert_eq!(converted.fields.len(), original.fields.len());

    for (orig, conv) in original.fields.iter().zip(converted.fields.iter()) {
        assert_eq!(orig.name, conv.name, "Field name mismatch");
        assert_eq!(orig.data_type, conv.data_type, "Data type mismatch");
        assert_eq!(orig.is_required, conv.is_required, "Required flag mismatch");
    }

    println!("[TEST] Roundtrip conversion successful");
}

#[test]
fn test_dtype_field_types() {
    let metadata = create_test_metadata();
    let dtype = metadata_to_dtype(&metadata).expect("Should convert");

    if let DType::Struct(struct_fields, _) = &dtype {
        // Check integer field
        let id_dtype = get_field_dtype_by_index(struct_fields, 0);
        assert!(matches!(
            id_dtype,
            Some(DType::Primitive(PType::I32, Nullability::NonNullable))
        ));

        // Check string field (nullable)
        let name_dtype = get_field_dtype_by_index(struct_fields, 1);
        assert!(matches!(
            name_dtype,
            Some(DType::Utf8(Nullability::Nullable))
        ));

        // Check boolean field
        let active_dtype = get_field_dtype_by_index(struct_fields, 2);
        assert!(matches!(
            active_dtype,
            Some(DType::Bool(Nullability::NonNullable))
        ));

        // Check double field (nullable)
        let score_dtype = get_field_dtype_by_index(struct_fields, 3);
        assert!(matches!(
            score_dtype,
            Some(DType::Primitive(PType::F64, Nullability::Nullable))
        ));
    }

    println!("[TEST] Field type verification successful");
}

#[test]
fn test_is_compatible_dtype() {
    let metadata = create_test_metadata();
    let dtype = metadata_to_dtype(&metadata).expect("Should convert");

    assert!(is_compatible_dtype(&dtype), "Should be compatible");

    // Test incompatible type
    let incompatible = DType::Primitive(PType::I32, Nullability::NonNullable);
    assert!(!is_compatible_dtype(&incompatible), "Non-struct should not be compatible");
}

#[test]
fn test_extended_types() {
    // Test all new types: byte, short, long, float, date, timestamp, binary, decimal
    let metadata = Metadata::new(
        "extended_types",
        vec![
            MetadataField { id: 1, name: "byte_col".to_string(), data_type: "byte".to_string(), is_required: true },
            MetadataField { id: 2, name: "short_col".to_string(), data_type: "short".to_string(), is_required: true },
            MetadataField { id: 3, name: "long_col".to_string(), data_type: "long".to_string(), is_required: true },
            MetadataField { id: 4, name: "float_col".to_string(), data_type: "float".to_string(), is_required: true },
            MetadataField { id: 5, name: "date_col".to_string(), data_type: "date".to_string(), is_required: false },
            MetadataField { id: 6, name: "timestamp_col".to_string(), data_type: "timestamp".to_string(), is_required: false },
            MetadataField { id: 7, name: "decimal_col".to_string(), data_type: "decimal".to_string(), is_required: false },
        ],
    );

    let dtype = metadata_to_dtype(&metadata).expect("Should convert extended types");

    if let DType::Struct(struct_fields, _) = &dtype {
        assert_eq!(struct_fields.names().len(), 7);

        // Verify byte -> I8
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 0),
            Some(DType::Primitive(PType::I8, Nullability::NonNullable))
        ));

        // Verify short -> I16
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 1),
            Some(DType::Primitive(PType::I16, Nullability::NonNullable))
        ));

        // Verify long -> I64
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 2),
            Some(DType::Primitive(PType::I64, Nullability::NonNullable))
        ));

        // Verify float -> F32
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 3),
            Some(DType::Primitive(PType::F32, Nullability::NonNullable))
        ));

        // Verify date -> I32 (days since epoch)
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 4),
            Some(DType::Primitive(PType::I32, Nullability::Nullable))
        ));

        // Verify timestamp -> I64 (millis since epoch)
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 5),
            Some(DType::Primitive(PType::I64, Nullability::Nullable))
        ));

        // Verify decimal -> Utf8
        assert!(matches!(
            get_field_dtype_by_index(struct_fields, 6),
            Some(DType::Utf8(Nullability::Nullable))
        ));
    }

    println!("[TEST] Extended types verification successful");
}

#[test]
fn test_date_timestamp_parsing() {
    // Test date parsing
    assert_eq!(parse_date_to_days("1970-01-01"), 0);
    assert_eq!(parse_date_to_days("1970-01-02"), 1);
    assert_eq!(parse_date_to_days("2024-01-01"), 19723); // Days from 1970 to 2024

    // Test timestamp parsing
    assert_eq!(parse_timestamp_to_millis("0"), 0);
    assert_eq!(parse_timestamp_to_millis("1000"), 1000);

    // ISO format
    let ts = parse_timestamp_to_millis("2024-01-01T00:00:00Z");
    assert!(ts > 0, "Should parse ISO format");

    // Datetime format
    let ts2 = parse_timestamp_to_millis("2024-01-01 12:30:45");
    assert!(ts2 > 0, "Should parse datetime format");

    println!("[TEST] Date/timestamp parsing successful");
}

#[test]
fn test_type_aliases() {
    // Test that aliases work: tinyint=byte, smallint=short, bigint=long, int=integer, bool=boolean
    let metadata = Metadata::new(
        "aliases",
        vec![
            MetadataField { id: 1, name: "a".to_string(), data_type: "tinyint".to_string(), is_required: true },
            MetadataField { id: 2, name: "b".to_string(), data_type: "smallint".to_string(), is_required: true },
            MetadataField { id: 3, name: "c".to_string(), data_type: "bigint".to_string(), is_required: true },
            MetadataField { id: 4, name: "d".to_string(), data_type: "int".to_string(), is_required: true },
            MetadataField { id: 5, name: "e".to_string(), data_type: "bool".to_string(), is_required: true },
        ],
    );

    let dtype = metadata_to_dtype(&metadata).expect("Should convert aliases");

    if let DType::Struct(struct_fields, _) = &dtype {
        assert!(matches!(get_field_dtype_by_index(struct_fields, 0), Some(DType::Primitive(PType::I8, _))));
        assert!(matches!(get_field_dtype_by_index(struct_fields, 1), Some(DType::Primitive(PType::I16, _))));
        assert!(matches!(get_field_dtype_by_index(struct_fields, 2), Some(DType::Primitive(PType::I64, _))));
        assert!(matches!(get_field_dtype_by_index(struct_fields, 3), Some(DType::Primitive(PType::I32, _))));
        assert!(matches!(get_field_dtype_by_index(struct_fields, 4), Some(DType::Bool(_))));
    }

    println!("[TEST] Type aliases verification successful");
}

#[test]
fn test_unsupported_type() {
    let metadata = Metadata::new(
        "test",
        vec![MetadataField {
            id: 1,
            name: "unknown".to_string(),
            data_type: "map".to_string(), // Complex types not supported
            is_required: true,
        }],
    );

    let result = metadata_to_dtype(&metadata);
    assert!(result.is_err(), "Should fail for unsupported type");

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Unsupported"), "Error should mention unsupported type");
}

#[test]
fn test_empty_metadata() {
    let metadata = Metadata::new("empty", vec![]);
    let dtype = metadata_to_dtype(&metadata).expect("Should handle empty metadata");

    if let DType::Struct(struct_fields, _) = dtype {
        assert_eq!(struct_fields.names().len(), 0);
    }
}

#[test]
fn test_vortex_version() {
    let version = vortex_version();
    assert!(!version.is_empty());
    println!("[TEST] Using Vortex version: {}", version);
}

// ========================================================================
// Phase 2: File I/O Tests
// ========================================================================

#[test]
fn test_build_struct_array() {
    let metadata = Metadata::new(
        "test",
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
    );

    let rows = vec!["1,Alice", "2,Bob", "3,Charlie"];
    let array = build_struct_array(&metadata, &rows).expect("Should build struct array");

    assert_eq!(array.len(), 3, "Should have 3 rows");
    println!("[TEST] Built struct array with {} rows", array.len());
}

#[tokio::test]
async fn test_write_and_read_vortex_file() {
    use tempfile::tempdir;

    let metadata = Metadata::new(
        "test_io",
        vec![
            MetadataField {
                id: 1,
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 2,
                name: "value".to_string(),
                data_type: "double".to_string(),
                is_required: false,
            },
        ],
    );

    let rows = vec!["1,10.5", "2,20.3", "3,30.7"];

    // Create temp directory and file path
    let temp_dir = tempdir().expect("Should create temp dir");
    let file_path = temp_dir.path().join("test.vtx");

    // Write file (use async version directly)
    let bytes_written = write_vortex_file_async(&file_path, &metadata, &rows).await
        .expect("Should write vortex file");
    assert!(bytes_written > 0, "Should write some bytes");
    println!("[TEST] Wrote {} bytes to Vortex file", bytes_written);

    // Read file (use async version directly)
    let (dtype, array) = read_vortex_file_async(&file_path).await
        .expect("Should read vortex file");

    // Verify schema from footer
    assert!(matches!(dtype, DType::Struct(_, _)), "Should read struct dtype");
    if let DType::Struct(fields, _) = &dtype {
        assert_eq!(fields.names().len(), 2, "Should have 2 fields");
        println!("[TEST] Read schema with {} fields from footer", fields.names().len());
    }

    // Verify data
    assert_eq!(array.len(), 3, "Should read 3 rows");
    println!("[TEST] Read {} rows from Vortex file", array.len());
}

#[tokio::test]
async fn test_write_all_types() {
    use tempfile::tempdir;

    let metadata = create_test_metadata(); // Has all 4 types
    let rows = vec![
        "1,Alice,true,95.5",
        "2,Bob,false,87.3",
        "3,Charlie,true,92.1",
    ];

    let temp_dir = tempdir().expect("Should create temp dir");
    let file_path = temp_dir.path().join("all_types.vtx");

    // Write (use async version directly)
    let bytes_written = write_vortex_file_async(&file_path, &metadata, &rows).await
        .expect("Should write all types");
    println!("[TEST] Wrote {} bytes with all types", bytes_written);

    // Read and verify (use async version directly)
    let (dtype, array) = read_vortex_file_async(&file_path).await
        .expect("Should read all types");

    if let DType::Struct(fields, _) = &dtype {
        assert_eq!(fields.names().len(), 4, "Should have 4 fields");

        // Verify field names
        assert_eq!(fields.names()[0].as_ref(), "id");
        assert_eq!(fields.names()[1].as_ref(), "name");
        assert_eq!(fields.names()[2].as_ref(), "active");
        assert_eq!(fields.names()[3].as_ref(), "score");
    }

    assert_eq!(array.len(), 3, "Should have 3 rows");
    println!("[TEST] Successfully wrote and read all data types");
}

#[tokio::test]
async fn test_schema_roundtrip_via_file() {
    use tempfile::tempdir;

    let original_metadata = create_test_metadata();
    let rows = vec!["1,Test,true,50.0"];

    let temp_dir = tempdir().expect("Should create temp dir");
    let file_path = temp_dir.path().join("schema_test.vtx");

    // Write file (use async version directly)
    write_vortex_file_async(&file_path, &original_metadata, &rows).await
        .expect("Should write file");

    // Read schema from file footer (use async version directly)
    let (dtype, _) = read_vortex_file_async(&file_path).await
        .expect("Should read file");

    // Convert back to metadata
    let recovered_metadata = dtype_to_metadata(&dtype, "recovered")
        .expect("Should convert dtype to metadata");

    // Verify schema matches
    assert_eq!(
        recovered_metadata.fields.len(),
        original_metadata.fields.len(),
        "Field count should match"
    );

    for (orig, recv) in original_metadata.fields.iter().zip(recovered_metadata.fields.iter()) {
        assert_eq!(orig.name, recv.name, "Field name should match");
        assert_eq!(orig.data_type, recv.data_type, "Data type should match");
    }

    println!("[TEST] Schema roundtrip via file successful");
}
