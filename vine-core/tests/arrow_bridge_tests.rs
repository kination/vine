use vine_core::arrow_bridge::{
    deserialize_arrow_ipc, serialize_arrow_ipc, arrow_schema_to_metadata,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_array::{Int32Array, StringArray, BooleanArray, RecordBatch};
use std::sync::Arc;

#[test]
fn test_arrow_ipc_serialization_roundtrip() {
    // Create a simple RecordBatch directly without CSV conversion
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
    ]);

    let id_array = Int32Array::from(vec![1]);
    let name_array = StringArray::from(vec![Some("Test")]);
    let active_array = BooleanArray::from(vec![true]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(active_array),
        ],
    ).expect("Should create batch");

    // Serialize to IPC
    let ipc_bytes = serialize_arrow_ipc(&batch).expect("Should serialize");
    assert!(!ipc_bytes.is_empty());

    // Deserialize from IPC
    let restored = deserialize_arrow_ipc(&ipc_bytes).expect("Should deserialize");
    assert_eq!(restored.num_rows(), 1);
    assert_eq!(restored.num_columns(), 3);
}

#[test]
fn test_arrow_schema_to_metadata() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
    ]);

    let metadata = arrow_schema_to_metadata(&schema, "test_table");

    assert_eq!(metadata.table_name, "test_table");
    assert_eq!(metadata.fields.len(), 3);

    assert_eq!(metadata.fields[0].name, "id");
    assert_eq!(metadata.fields[0].data_type, "integer");
    assert!(metadata.fields[0].is_required);

    assert_eq!(metadata.fields[1].name, "name");
    assert_eq!(metadata.fields[1].data_type, "string");
    assert!(!metadata.fields[1].is_required);

    assert_eq!(metadata.fields[2].name, "active");
    assert_eq!(metadata.fields[2].data_type, "boolean");
    assert!(metadata.fields[2].is_required);
}
