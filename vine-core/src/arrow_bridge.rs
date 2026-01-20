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
/// # Deprecated
/// This is only used by the deprecated CSV bridge functions.
/// Will be removed when direct Arrow->Vortex conversion is implemented.
#[deprecated(since = "0.2.0", note = "Only used by CSV bridge. Will be removed with direct Arrow->Vortex conversion.")]
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

/// Convert RecordBatch to CSV rows for Vortex writer
///
/// # Deprecated
/// This function is a temporary bridge between Arrow IPC and CSV-based Vortex writer.
/// Will be replaced with direct Arrow → Vortex conversion in v0.3.0.
///
/// This bridges Arrow IPC data to the existing Vortex writer that expects CSV.
/// Future optimization: Direct Arrow -> Vortex conversion without CSV intermediate.
#[deprecated(since = "0.2.0", note = "Temporary CSV bridge. Direct Arrow->Vortex conversion coming in v0.3.0. Adds 20-30% overhead.")]
pub fn record_batch_to_csv_rows(batch: &RecordBatch) -> ArrowBridgeResult<Vec<String>> {
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
/// # Deprecated
/// This function is a temporary bridge between CSV-based reader and Arrow IPC.
/// It has be replaced with direct 'Vortex → Arrow' conversion since v0.3.0.
#[deprecated(since = "0.2.0", note = "Temporary CSV bridge. Direct Vortex->Arrow conversion coming in v0.3.0. Adds 20-30% overhead.")]
pub fn csv_rows_to_record_batch(
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

