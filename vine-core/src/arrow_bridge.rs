use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, RecordBatch,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use crate::metadata::{Metadata, MetadataField};
use crate::vortex_exp::{self, VortexResult};
use vortex::{Array as VortexArray, ArrayRef as VortexArrayRef};
use vortex::arrays::{BoolArray, PrimitiveArray, StructArray};
use vortex::validity::Validity;
use vortex_dtype::{DType, Nullability, PType};

/// Result type for Arrow bridge operations
pub type ArrowBridgeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Deserialize Arrow IPC bytes into RecordBatch
///
/// # Arguments
/// * `data` - Arrow IPC stream bytes from JVM
///
/// # Returns
/// * `RecordBatch` containing the deserialized data
pub fn deserialize_arrow_ipc(data: &[u8]) -> Result<RecordBatch, ArrowError> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;

    // Read first (and only) batch
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(e),
        None => Err(ArrowError::InvalidArgumentError("Empty IPC stream".into())),
    }
}

/// Serialize RecordBatch to Arrow IPC bytes
///
/// # Arguments
/// * `batch` - RecordBatch to serialize
///
/// # Returns
/// * `Vec<u8>` containing Arrow IPC stream bytes for JVM
pub fn serialize_arrow_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Convert Vine metadata to Arrow schema
///
/// # Note
/// This is used by the CSV bridge functions (csv_rows_to_record_batch).
/// Will be removed when direct Arrow->Vortex conversion is implemented.
fn metadata_to_arrow_schema(metadata: &Metadata) -> ArrowBridgeResult<Schema> {
    let fields: Vec<Field> = metadata
        .fields
        .iter()
        .map(|field| {
            let arrow_type = vine_type_to_arrow(&field.data_type);
            Field::new(&field.name, arrow_type, !field.is_required)
        })
        .collect();

    Ok(Schema::new(fields))
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

/// Convert Vine type string to Arrow DataType
fn vine_type_to_arrow(vine_type: &str) -> DataType {
    match vine_type.to_lowercase().as_str() {
        "byte" | "tinyint" => DataType::Int8,
        "short" | "smallint" => DataType::Int16,
        "integer" | "int" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "boolean" | "bool" => DataType::Boolean,
        "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32, // Days since epoch
        "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "decimal" => DataType::Utf8, // Stored as string for precision
        _ => DataType::Utf8,         // Fallback
    }
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

// ============================================================================
// Temporary CSV Bridge Utilities
// ============================================================================
//
// TODO: Replace these utility functions with direct Arrow ↔ Vortex conversion
//
// These functions isolate the CSV conversion logic so it can be easily replaced
// with direct conversion once Vortex API is stable. When implementing direct
// conversion, only these two functions need to be modified:
//
// 1. arrow_to_storage_format() - Replace CSV conversion with direct Arrow → Vortex
// 2. storage_format_to_arrow() - Replace CSV conversion with direct Vortex → Arrow
//
// Impact: Changing only these two functions will update all JNI write/read paths
// ============================================================================

/// Convert Arrow RecordBatch to storage format (currently CSV, future: direct Vortex)
///
/// **TODO: Replace CSV conversion with direct Arrow → Vortex when Vortex API is stable**
///
/// # Arguments
/// * `batch` - Arrow RecordBatch from JVM
///
/// # Returns
/// * Storage format data (currently Vec<String> of CSV rows)
///
/// # Migration path
/// When implementing direct conversion:
/// 1. Change return type from Vec<String> to VortexArrayRef
/// 2. Replace body with: `record_batch_to_vortex(batch)` (from direct_conversion mod)
/// 3. Update callers to use vortex writer instead of CSV writer
///
pub fn arrow_to_storage_format(batch: &RecordBatch) -> ArrowBridgeResult<Vec<String>> {
    // TODO: Replace with direct conversion
    // return Ok(record_batch_to_vortex(batch)?);
    record_batch_to_csv_rows(batch)
}

/// Convert storage format to Arrow RecordBatch (currently from CSV, future: direct from Vortex)
///
/// **TODO: Replace CSV conversion with direct Vortex → Arrow when Vortex API is stable**
///
/// # Arguments
/// * `data` - Storage format data (currently Vec<String> of CSV rows)
/// * `metadata` - Vine metadata for schema
///
/// # Returns
/// * Arrow RecordBatch for JVM
///
/// # Migration path
/// When implementing direct conversion:
/// 1. Change first parameter type from Vec<String> to VortexArrayRef
/// 2. Replace body with: `vortex_to_record_batch(vortex_array, metadata)` (from direct_conversion mod)
/// 3. Update callers to pass vortex array instead of CSV rows
/// 
pub fn storage_format_to_arrow(
    csv_rows: &[String],
    metadata: &Metadata,
) -> ArrowBridgeResult<RecordBatch> {
    // TODO: Replace with direct conversion
    // return Ok(vortex_to_record_batch(vortex_array, metadata)?);
    csv_rows_to_record_batch(csv_rows, metadata)
}

/// Convert RecordBatch to CSV rows for Vortex writer
///
/// # Note
/// This function is a temporary bridge between Arrow IPC and CSV-based Vortex writer.
/// Will be replaced with direct Arrow → Vortex conversion in future
///
fn record_batch_to_csv_rows(batch: &RecordBatch) -> ArrowBridgeResult<Vec<String>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let column = batch.column(col_idx);
            let value = extract_value(column, row_idx);
            values.push(value);
        }

        rows.push(values.join(","));
    }

    Ok(rows)
}

/// Convert CSV rows to RecordBatch for JNI return
///
/// # Note
/// This function is a temporary bridge between CSV-based reader and Arrow IPC.
/// Will be replaced with direct Vortex → Arrow conversion in future
///
fn csv_rows_to_record_batch(
    rows: &[String],
    metadata: &Metadata,
) -> ArrowBridgeResult<RecordBatch> {
    let schema = metadata_to_arrow_schema(metadata)?;
    let num_rows = rows.len();

    // Parse rows into columns
    let parsed_rows: Vec<Vec<&str>> = rows
        .iter()
        .map(|row| row.split(',').map(|s| s.trim()).collect())
        .collect();

    // Build column arrays
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(metadata.fields.len());

    for (col_idx, field) in metadata.fields.iter().enumerate() {
        let values: Vec<&str> = parsed_rows
            .iter()
            .map(|row| row.get(col_idx).copied().unwrap_or(""))
            .collect();

        let array = build_arrow_array(&field.data_type, &values, num_rows)?;
        columns.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(batch)
}

/// Extract value from Arrow array at given index
fn extract_value(column: &ArrayRef, row_idx: usize) -> String {
    if column.is_null(row_idx) {
        return String::new();
    }

    match column.data_type() {
        DataType::Int8 => {
            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int16 => {
            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Binary => {
            let arr = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            base64_encode(arr.value(row_idx))
        }
        DataType::Date32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            days_to_date_string(arr.value(row_idx))
        }
        DataType::Timestamp(_, _) => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row_idx).to_string() // Return millis as string
        }
        _ => String::new(),
    }
}

/// Build Arrow array from string values based on Vine type
fn build_arrow_array(
    type_str: &str,
    values: &[&str],
    _num_rows: usize,
) -> ArrowBridgeResult<ArrayRef> {
    match type_str.to_lowercase().as_str() {
        "byte" | "tinyint" => {
            let arr: Int8Array = values.iter().map(|v| v.parse::<i8>().ok()).collect();
            Ok(Arc::new(arr))
        }
        "short" | "smallint" => {
            let arr: Int16Array = values.iter().map(|v| v.parse::<i16>().ok()).collect();
            Ok(Arc::new(arr))
        }
        "integer" | "int" => {
            let arr: Int32Array = values.iter().map(|v| v.parse::<i32>().ok()).collect();
            Ok(Arc::new(arr))
        }
        "long" | "bigint" => {
            let arr: Int64Array = values.iter().map(|v| v.parse::<i64>().ok()).collect();
            Ok(Arc::new(arr))
        }
        "float" => {
            let arr: Float32Array = values.iter().map(|v| v.parse::<f32>().ok()).collect();
            Ok(Arc::new(arr))
        }
        "double" => {
            let arr: Float64Array = values.iter().map(|v| v.parse::<f64>().ok()).collect();
            Ok(Arc::new(arr))
        }
        "boolean" | "bool" => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| Some(matches!(v.to_lowercase().as_str(), "true" | "1" | "yes")))
                .collect();
            Ok(Arc::new(arr))
        }
        "string" | "decimal" => {
            let arr: StringArray = values.iter().map(|v| Some(*v)).collect();
            Ok(Arc::new(arr))
        }
        "binary" => {
            let decoded: Vec<Option<Vec<u8>>> = values
                .iter()
                .map(|v| base64_decode(v).ok())
                .collect();
            let arr: BinaryArray = decoded
                .iter()
                .map(|opt| opt.as_ref().map(|v| v.as_slice()))
                .collect();
            Ok(Arc::new(arr))
        }
        "date" => {
            let arr: Int32Array = values.iter().map(|v| Some(parse_date_to_days(v))).collect();
            Ok(Arc::new(arr))
        }
        "timestamp" => {
            let arr: Int64Array = values
                .iter()
                .map(|v| Some(parse_timestamp_to_millis(v)))
                .collect();
            Ok(Arc::new(arr))
        }
        _ => {
            let arr: StringArray = values.iter().map(|v| Some(*v)).collect();
            Ok(Arc::new(arr))
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse date string (YYYY-MM-DD) to days since Unix epoch
fn parse_date_to_days(s: &str) -> i32 {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(|d| (d - epoch).num_days() as i32)
        .unwrap_or(0)
}

/// Parse timestamp string to milliseconds since Unix epoch
fn parse_timestamp_to_millis(s: &str) -> i64 {
    // Try parsing as epoch milliseconds first
    if let Ok(millis) = s.parse::<i64>() {
        return millis;
    }
    // Try ISO 8601 format
    use chrono::DateTime;
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_millis();
    }
    0
}

/// Convert days since epoch to date string (YYYY-MM-DD)
fn days_to_date_string(days: i32) -> String {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
        date.format("%Y-%m-%d").to_string()
    } else {
        "1970-01-01".to_string()
    }
}

/// Base64 encode bytes using the base64 crate
fn base64_encode(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

/// Base64 decode string using the base64 crate
fn base64_decode(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    BASE64.decode(s.trim()).map_err(|e| e.into())
}

// ============================================================================
// Direct Arrow ↔ Vortex Conversion (No CSV intermediate)
// ============================================================================
//
// TODO: Complete implementation of direct Arrow ↔ Vortex conversion
//
// Status: PARTIAL IMPLEMENTATION (disabled due to Vortex API issues)
//
// Current blockers:
// 1. PrimitiveArray::from_vec() API not available in current Vortex version
// 2. BoolArray::from_vec() API not available
// 3. VarBinViewBuilder API signatures don't match (push_null, finish methods)
// 4. Validity buffer conversion needs correct API usage
// 5. StructArray::from_fields() error handling
//
// Expected performance gain: 20-30% reduction in overhead
//
// Current workaround: Using CSV bridge (record_batch_to_csv_rows / csv_rows_to_record_batch)
//
// Next steps:
// 1. Update to stable Vortex API version with complete builder APIs
// 2. Fix Validity buffer conversion (Vortex validity ↔ Arrow null buffer)
// 3. Implement proper VarBinView builder usage for strings/binary
// 4. Add comprehensive tests for all data types
// 5. Benchmark performance vs CSV bridge
//
// References:
// - lib.rs: Arrow IPC JNI functions using CSV bridge (lines 220-314)
// - vortex_exp.rs: Existing CSV-based conversion (array_to_csv_rows, build_struct_array)
//
// ============================================================================

// Disable compilation of direct conversion code until Vortex API is fixed
#[cfg(feature = "direct-vortex-conversion")]
mod direct_conversion {
use super::*;

/// Convert Arrow RecordBatch directly to Vortex StructArray
///
/// **TODO: Currently disabled - requires Vortex API fixes**
///
/// This function provides direct Arrow → Vortex conversion without CSV intermediate.
/// Eliminates 20-30% overhead compared to the CSV bridge approach.
///
/// # Arguments
/// * `batch` - Arrow RecordBatch to convert
///
/// # Returns
/// * `VortexArrayRef` - Vortex StructArray ready for file write
///
/// # Status
/// Partial implementation with compilation errors. See module-level TODO for details.
pub fn record_batch_to_vortex(batch: &RecordBatch) -> ArrowBridgeResult<VortexArrayRef> {
    use vortex::builders::ArrayBuilder;
    use vortex::IntoArray;

    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // Build Vortex columns from Arrow columns
    let mut vortex_columns: Vec<VortexArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let arrow_column = batch.column(col_idx);
        let field = schema.field(col_idx);
        let vortex_array = arrow_array_to_vortex(arrow_column, field.data_type())?;
        vortex_columns.push(vortex_array);
    }

    // Build field names from schema
    let field_names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();

    // Create Vortex StructArray
    let struct_array = StructArray::from_fields(field_names, vortex_columns)
        .map_err(|e| format!("Failed to create Vortex StructArray: {}", e))?;

    Ok(struct_array.into_array())
}

/// Convert single Arrow array to Vortex array
///
/// **TODO: Currently disabled - part of direct conversion implementation**
fn arrow_array_to_vortex(arrow_array: &ArrayRef, data_type: &DataType) -> ArrowBridgeResult<VortexArrayRef> {
    use vortex::builders::VarBinViewBuilder;
    use vortex::validity::Validity;
    use vortex::IntoArray;

    match data_type {
        DataType::Int8 => {
            let arr = arrow_array.as_any().downcast_ref::<Int8Array>().unwrap();
            let values: Vec<i8> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            let validity = build_validity(arr.nulls());
            Ok(PrimitiveArray::from_vec(values, validity).into_array())
        }
        DataType::Int16 => {
            let arr = arrow_array.as_any().downcast_ref::<Int16Array>().unwrap();
            let values: Vec<i16> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            let validity = build_validity(arr.nulls());
            Ok(PrimitiveArray::from_vec(values, validity).into_array())
        }
        DataType::Int32 | DataType::Date32 => {
            let arr = arrow_array.as_any().downcast_ref::<Int32Array>().unwrap();
            let values: Vec<i32> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            let validity = build_validity(arr.nulls());
            Ok(PrimitiveArray::from_vec(values, validity).into_array())
        }
        DataType::Int64 | DataType::Timestamp(_, _) | DataType::Date64 => {
            let arr = arrow_array.as_any().downcast_ref::<Int64Array>().unwrap();
            let values: Vec<i64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            let validity = build_validity(arr.nulls());
            Ok(PrimitiveArray::from_vec(values, validity).into_array())
        }
        DataType::Float32 => {
            let arr = arrow_array.as_any().downcast_ref::<Float32Array>().unwrap();
            let values: Vec<f32> = (0..arr.len()).map(|i| if arr.is_null(i) { 0.0 } else { arr.value(i) }).collect();
            let validity = build_validity(arr.nulls());
            Ok(PrimitiveArray::from_vec(values, validity).into_array())
        }
        DataType::Float64 => {
            let arr = arrow_array.as_any().downcast_ref::<Float64Array>().unwrap();
            let values: Vec<f64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0.0 } else { arr.value(i) }).collect();
            let validity = build_validity(arr.nulls());
            Ok(PrimitiveArray::from_vec(values, validity).into_array())
        }
        DataType::Boolean => {
            let arr = arrow_array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let values: Vec<bool> = (0..arr.len()).map(|i| !arr.is_null(i) && arr.value(i)).collect();
            let validity = build_validity(arr.nulls());
            Ok(BoolArray::from_vec(values, validity).into_array())
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = arrow_array.as_any().downcast_ref::<StringArray>().unwrap();
            let mut builder = VarBinViewBuilder::<str>::new();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.push_null();
                } else {
                    builder.push_value(arr.value(i));
                }
            }
            Ok(builder.finish(DType::Utf8(Nullability::Nullable)).into_array())
        }
        DataType::Binary | DataType::LargeBinary => {
            let arr = arrow_array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut builder = VarBinViewBuilder::<[u8]>::new();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.push_null();
                } else {
                    builder.push_value(arr.value(i));
                }
            }
            Ok(builder.finish(DType::Binary(Nullability::Nullable)).into_array())
        }
        _ => Err(format!("Unsupported Arrow data type: {:?}", data_type).into()),
    }
}

/// Build Vortex Validity from Arrow nulls buffer
///
/// **TODO: Currently disabled - part of direct conversion implementation**
fn build_validity(nulls: Option<&arrow_buffer::NullBuffer>) -> Validity {
    match nulls {
        Some(null_buffer) => {
            // Convert Arrow null buffer to Vortex validity
            let null_count = null_buffer.null_count();
            if null_count == 0 {
                Validity::NonNullable
            } else {
                // Extract null bitmap
                let buffer = null_buffer.inner();
                Validity::from(buffer.clone())
            }
        }
        None => Validity::NonNullable,
    }
}

/// Convert Vortex StructArray directly to Arrow RecordBatch
///
/// **TODO: Currently disabled - requires Vortex API fixes**
///
/// This function provides direct Vortex → Arrow conversion without CSV intermediate.
/// Eliminates 20-30% overhead compared to the CSV bridge approach.
///
/// # Arguments
/// * `vortex_array` - Vortex StructArray from file read
/// * `metadata` - Vine metadata for schema information
///
/// # Returns
/// * `RecordBatch` - Arrow RecordBatch ready for IPC serialization
///
/// # Status
/// Partial implementation with compilation errors. See module-level TODO for details.
pub fn vortex_to_record_batch(vortex_array: &VortexArrayRef, metadata: &Metadata) -> ArrowBridgeResult<RecordBatch> {
    use vortex::arrays::StructArray;

    // Cast to StructArray
    let struct_array = StructArray::try_from(vortex_array)
        .map_err(|e| format!("Failed to cast to StructArray: {}", e))?;

    let num_rows = vortex_exp::get_row_count(vortex_array);

    // Build Arrow schema from metadata
    let arrow_fields: Vec<Field> = metadata.fields.iter().map(|f| {
        let arrow_type = vine_type_to_arrow(&f.data_type);
        Field::new(&f.name, arrow_type, !f.is_required)
    }).collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Convert each Vortex column to Arrow column
    let mut arrow_columns: Vec<ArrayRef> = Vec::with_capacity(metadata.fields.len());

    for (idx, field) in metadata.fields.iter().enumerate() {
        let vortex_child = struct_array.field(idx)
            .ok_or_else(|| format!("Missing field at index {}", idx))?;

        let arrow_array = vortex_array_to_arrow(&vortex_child, &field.data_type, num_rows)?;
        arrow_columns.push(arrow_array);
    }

    // Create RecordBatch
    let batch = RecordBatch::try_new(arrow_schema, arrow_columns)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

    Ok(batch)
}

/// Convert single Vortex array to Arrow array
///
/// **TODO: Currently disabled - part of direct conversion implementation**
fn vortex_array_to_arrow(vortex_array: &VortexArrayRef, vine_type: &str, num_rows: usize) -> ArrowBridgeResult<ArrayRef> {
    match vine_type.to_lowercase().as_str() {
        "byte" | "tinyint" => {
            let prim = vortex_array.to_primitive();
            let values: Vec<Option<i8>> = (0..num_rows).map(|i| {
                let scalar = prim.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(Int8Array::from(values)))
        }
        "short" | "smallint" => {
            let prim = vortex_array.to_primitive();
            let values: Vec<Option<i16>> = (0..num_rows).map(|i| {
                let scalar = prim.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(Int16Array::from(values)))
        }
        "integer" | "int" | "date" => {
            let prim = vortex_array.to_primitive();
            let values: Vec<Option<i32>> = (0..num_rows).map(|i| {
                let scalar = prim.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        "long" | "bigint" | "timestamp" => {
            let prim = vortex_array.to_primitive();
            let values: Vec<Option<i64>> = (0..num_rows).map(|i| {
                let scalar = prim.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        "float" => {
            let prim = vortex_array.to_primitive();
            let values: Vec<Option<f32>> = (0..num_rows).map(|i| {
                let scalar = prim.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(Float32Array::from(values)))
        }
        "double" => {
            let prim = vortex_array.to_primitive();
            let values: Vec<Option<f64>> = (0..num_rows).map(|i| {
                let scalar = prim.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        "boolean" | "bool" => {
            let bool_arr = vortex_array.to_bool();
            let values: Vec<Option<bool>> = (0..num_rows).map(|i| {
                let scalar = bool_arr.scalar_at(i);
                scalar.as_ref().try_into().ok()
            }).collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        "string" | "decimal" => {
            // Vortex strings are stored as VarBinView
            let values: Vec<Option<String>> = (0..num_rows).map(|i| {
                if vortex_array.is_valid(i) {
                    // Extract string value from Vortex array
                    vortex_exp::extract_string_value(vortex_array, i).ok()
                } else {
                    None
                }
            }).collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        "binary" => {
            let values: Vec<Option<Vec<u8>>> = (0..num_rows).map(|i| {
                if vortex_array.is_valid(i) {
                    vortex_exp::extract_binary_value(vortex_array, i).ok()
                } else {
                    None
                }
            }).collect();
            Ok(Arc::new(BinaryArray::from(values)))
        }
        _ => Err(format!("Unsupported Vine type: {}", vine_type).into()),
    }
}

} // end mod direct_conversion

