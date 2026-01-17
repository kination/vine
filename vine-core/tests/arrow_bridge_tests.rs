use vine_core::arrow_bridge::{
    csv_rows_to_record_batch, deserialize_arrow_ipc, metadata_to_arrow_schema,
    record_batch_to_csv_rows, serialize_arrow_ipc, arrow_schema_to_metadata,
};
use vine_core::metadata::{Metadata, MetadataField};
use arrow_schema::DataType;

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
        ],
    )
}

#[test]
fn test_metadata_to_arrow_schema() {
    let metadata = create_test_metadata();
    let schema = metadata_to_arrow_schema(&metadata).expect("Should convert");

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(*schema.field(0).data_type(), DataType::Int32);
    assert!(!schema.field(0).is_nullable());

    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    assert!(schema.field(1).is_nullable());

    assert_eq!(schema.field(2).name(), "active");
    assert_eq!(*schema.field(2).data_type(), DataType::Boolean);
}

#[test]
fn test_arrow_schema_to_metadata_roundtrip() {
    let original = create_test_metadata();
    let schema = metadata_to_arrow_schema(&original).expect("Should convert to schema");
    let converted = arrow_schema_to_metadata(&schema, "converted");

    assert_eq!(converted.fields.len(), original.fields.len());
    for (orig, conv) in original.fields.iter().zip(converted.fields.iter()) {
        assert_eq!(orig.name, conv.name);
        assert_eq!(orig.data_type, conv.data_type);
        assert_eq!(orig.is_required, conv.is_required);
    }
}

#[test]
fn test_csv_to_record_batch_roundtrip() {
    let metadata = create_test_metadata();
    let csv_rows = vec![
        "1,Alice,true".to_string(),
        "2,Bob,false".to_string(),
        "3,Charlie,true".to_string(),
    ];

    // CSV -> RecordBatch
    let batch = csv_rows_to_record_batch(&csv_rows, &metadata).expect("Should convert");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3);

    // RecordBatch -> CSV
    let back_to_csv = record_batch_to_csv_rows(&batch).expect("Should convert back");
    assert_eq!(back_to_csv.len(), 3);
    assert_eq!(back_to_csv[0], "1,Alice,true");
    assert_eq!(back_to_csv[1], "2,Bob,false");
    assert_eq!(back_to_csv[2], "3,Charlie,true");
}

#[test]
fn test_arrow_ipc_serialization_roundtrip() {
    let metadata = create_test_metadata();
    let csv_rows = vec!["1,Test,true".to_string()];

    let batch = csv_rows_to_record_batch(&csv_rows, &metadata).expect("Should create batch");

    // Serialize to IPC
    let ipc_bytes = serialize_arrow_ipc(&batch).expect("Should serialize");
    assert!(!ipc_bytes.is_empty());

    // Deserialize from IPC
    let restored = deserialize_arrow_ipc(&ipc_bytes).expect("Should deserialize");
    assert_eq!(restored.num_rows(), 1);
    assert_eq!(restored.num_columns(), 3);
}

#[test]
fn test_all_vine_types() {
    let metadata = Metadata::new(
        "all_types",
        vec![
            MetadataField {
                id: 1,
                name: "byte_col".to_string(),
                data_type: "byte".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 2,
                name: "short_col".to_string(),
                data_type: "short".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 3,
                name: "int_col".to_string(),
                data_type: "integer".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 4,
                name: "long_col".to_string(),
                data_type: "long".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 5,
                name: "float_col".to_string(),
                data_type: "float".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 6,
                name: "double_col".to_string(),
                data_type: "double".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 7,
                name: "bool_col".to_string(),
                data_type: "boolean".to_string(),
                is_required: true,
            },
            MetadataField {
                id: 8,
                name: "string_col".to_string(),
                data_type: "string".to_string(),
                is_required: true,
            },
        ],
    );

    let csv_rows = vec!["127,32767,2147483647,9223372036854775807,3.14,2.718,true,hello".to_string()];

    let batch = csv_rows_to_record_batch(&csv_rows, &metadata).expect("Should handle all types");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 8);

    // Verify IPC roundtrip
    let ipc_bytes = serialize_arrow_ipc(&batch).expect("Should serialize");
    let restored = deserialize_arrow_ipc(&ipc_bytes).expect("Should deserialize");
    assert_eq!(restored.num_rows(), 1);
}
