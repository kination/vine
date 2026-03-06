use std::io::Cursor;
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_cast::cast;

use vortex::ArrayRef as VortexArrayRef;
use vortex::arrow::FromArrowArray;

use crate::metadata::{Metadata, MetadataField};

/// Result type for Arrow bridge operations
pub type ArrowBridgeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

// ============================================================================
// Arrow IPC Serialization
// ============================================================================

/// Deserialize Arrow IPC bytes into RecordBatch
pub fn deserialize_arrow_ipc(data: &[u8]) -> Result<RecordBatch, ArrowError> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;

    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(e),
        None => Err(ArrowError::InvalidArgumentError("Empty IPC stream".into())),
    }
}

/// Serialize RecordBatch to Arrow IPC bytes
pub fn serialize_arrow_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

// ============================================================================
// Direct Arrow ↔ Vortex Conversion
// ============================================================================

/// Convert Arrow RecordBatch directly to Vortex ArrayRef
///
/// Uses Vortex native `FromArrowArray` trait for zero-copy conversion
pub fn arrow_to_vortex(batch: &RecordBatch) -> ArrowBridgeResult<VortexArrayRef> {
    Ok(VortexArrayRef::from_arrow(batch, false))
}

/// Convert Vortex ArrayRef directly to Arrow RecordBatch
///
/// # Arguments
/// * `vortex_array` - Source Vortex array
/// * `compat_mode` - If true, converts modern Arrow types (e.g. Utf8View) to legacy types (e.g. Utf8)
pub fn vortex_to_arrow(vortex_array: &VortexArrayRef, compat_mode: bool) -> ArrowBridgeResult<RecordBatch> {
    match RecordBatch::try_from(vortex_array.as_ref()) {
        Ok(batch) => {
            if compat_mode {
                // Ensure compatibility with older Arrow implementations (e.g. Java 14, Spark 3.5)
                make_batch_compatible(&batch)
            } else {
                Ok(batch)
            }
        }
        Err(e) => Err(format!("Failed to convert Vortex array to Arrow RecordBatch: {}", e).into()),
    }
}

/// Ensure RecordBatch is compatible with older Arrow implementations (e.g. Java 14).
///
/// Converts modern types (Utf8View, BinaryView) back to standard Utf8/Binary.
pub fn make_batch_compatible(batch: &RecordBatch) -> ArrowBridgeResult<RecordBatch> {
    let mut new_columns = Vec::new();
    let mut new_fields = Vec::new();
    let mut changed = false;

    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let (new_field, new_column) = match field.data_type() {
            DataType::Utf8View => {
                changed = true;
                let casted = cast(column, &DataType::Utf8)?;
                let mut f = field.as_ref().clone();
                f.set_data_type(DataType::Utf8);
                (Arc::new(f), casted)
            }
            DataType::BinaryView => {
                changed = true;
                let casted = cast(column, &DataType::Binary)?;
                let mut f = field.as_ref().clone();
                f.set_data_type(DataType::Binary);
                (Arc::new(f), casted)
            }
            _ => (field.clone(), column.clone()),
        };
        new_fields.push(new_field);
        new_columns.push(new_column);
    }

    if changed {
        let new_schema = Arc::new(Schema::new(new_fields));
        Ok(RecordBatch::try_new(new_schema, new_columns)?)
    } else {
        Ok(batch.clone())
    }
}

/// Convert Arrow schema to Vine metadata
pub fn arrow_schema_to_metadata(schema: &Schema, table_name: &str) -> Metadata {
    let fields: Vec<MetadataField> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let data_type = arrow_type_to_vine(field.data_type());
            MetadataField {
                id: (idx + 1) as i32,
                name: field.name().clone(),
                data_type,
                is_required: !field.is_nullable(),
            }
        })
        .collect();

    Metadata::new(table_name, fields)
}

/// Convert Arrow DataType to Vine type string
fn arrow_type_to_vine(arrow_type: &DataType) -> String {
    match arrow_type {
        DataType::Int8 => "byte".to_string(),
        DataType::Int16 => "short".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64 => "long".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Binary | DataType::LargeBinary => "binary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        _ => "string".to_string(), // Fallback
    }
}
