/// Provides Vortex-based file I/O for Vine.
/// - DType conversion between Vine metadata and Vortex
/// - File read/write with date partitioning
/// - CSV ↔ Vortex array conversion for JNI
///
use std::path::Path;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures::StreamExt;
use tokio::runtime::Runtime;
use vortex::arrays::{BoolArray, PrimitiveArray, StructArray};
use vortex::builders::{ArrayBuilder, VarBinViewBuilder};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::validity::Validity;
use vortex::{Array, ArrayRef, IntoArray};
use vortex::VortexSessionDefault;
use vortex_dtype::{DType, FieldName, FieldNames, Nullability, PType, StructFields};

// File I/O traits from vortex_file (re-exported through vortex::file)
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};

use crate::metadata::{Metadata, MetadataField};

/// Result type for Vortex operations
pub type VortexResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Convert Vine metadata schema to Vortex DType
///
/// Maps Vine data types to Vortex DTypes:
///
/// **Integer types:**
/// - "byte", "tinyint" -> PType::I8
/// - "short", "smallint" -> PType::I16
/// - "integer", "int" -> PType::I32
/// - "long", "bigint" -> PType::I64
///
/// **Floating point types:**
/// - "float" -> PType::F32
/// - "double" -> PType::F64
///
/// **Other primitive types:**
/// - "boolean", "bool" -> Bool
/// - "string" -> Utf8
/// - "binary" -> Binary
///
/// **Date/Time types:**
/// - "date" -> Extension (stored as I32, days since epoch)
/// - "timestamp" -> Extension (stored as I64, milliseconds since epoch)
///
/// **Numeric types:**
/// - "decimal" -> Extension (stored as Utf8 for precision)
///
/// # Example
/// ```ignore
/// use vine_core::metadata::{Metadata, MetadataField};
/// use vine_core::vortex_exp::metadata_to_dtype;
///
/// let metadata = Metadata::new("test", vec![
///     MetadataField { id: 1, name: "id".into(), data_type: "integer".into(), is_required: true },
/// ]);
/// let dtype = metadata_to_dtype(&metadata).unwrap();
/// ```
pub fn metadata_to_dtype(metadata: &Metadata) -> VortexResult<DType> {
    let mut field_names: Vec<FieldName> = Vec::new();
    let mut field_types: Vec<DType> = Vec::new();

    for field in &metadata.fields {
        field_names.push(FieldName::from(field.name.clone()));

        let nullability = if field.is_required {
            Nullability::NonNullable
        } else {
            Nullability::Nullable
        };

        let dtype = vine_type_to_dtype(&field.data_type, nullability)?;
        field_types.push(dtype);
    }

    let struct_fields = StructFields::new(FieldNames::from(field_names), field_types.into());

    Ok(DType::Struct(
        struct_fields.into(),
        Nullability::NonNullable,
    ))
}

/// Convert Vine type string to Vortex DType
fn vine_type_to_dtype(type_str: &str, nullability: Nullability) -> VortexResult<DType> {
    match type_str.to_lowercase().as_str() {
        // Integer types
        "byte" | "tinyint" => Ok(DType::Primitive(PType::I8, nullability)),
        "short" | "smallint" => Ok(DType::Primitive(PType::I16, nullability)),
        "integer" | "int" => Ok(DType::Primitive(PType::I32, nullability)),
        "long" | "bigint" => Ok(DType::Primitive(PType::I64, nullability)),

        // Floating point types
        "float" => Ok(DType::Primitive(PType::F32, nullability)),
        "double" => Ok(DType::Primitive(PType::F64, nullability)),

        // Other primitive types
        "boolean" | "bool" => Ok(DType::Bool(nullability)),
        "string" => Ok(DType::Utf8(nullability)),
        "binary" => Ok(DType::Binary(nullability)),

        // Date/Time types - stored as primitives with semantic meaning
        // Date: days since Unix epoch (1970-01-01)
        "date" => Ok(DType::Primitive(PType::I32, nullability)),
        // Timestamp: milliseconds since Unix epoch
        "timestamp" => Ok(DType::Primitive(PType::I64, nullability)),

        // Decimal: stored as string for precision preservation
        "decimal" => Ok(DType::Utf8(nullability)),

        other => Err(format!("Unsupported data type: {}", other).into()),
    }
}

/// Convert Vortex DType back to Vine Metadata
///
/// This enables reading schema from Vortex file footer and converting
/// it back to Vine's metadata format.
pub fn dtype_to_metadata(dtype: &DType, table_name: &str) -> VortexResult<Metadata> {
    match dtype {
        DType::Struct(struct_fields, _) => {
            let names = struct_fields.names();

            let fields: Vec<MetadataField> = names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    // Get field dtype by looking at the struct's field types
                    let field_dtype = get_field_dtype_by_index(struct_fields, i);
                    let (data_type, is_required) = dtype_to_vine_type(field_dtype);

                    MetadataField {
                        id: (i + 1) as i32,
                        name: name.to_string(),
                        data_type,
                        is_required,
                    }
                })
                .collect();

            Ok(Metadata::new(table_name, fields))
        }
        _ => Err("DType must be a Struct for table schema".into()),
    }
}

/// Convert Vortex DType to Vine type string
fn dtype_to_vine_type(dtype: Option<DType>) -> (String, bool) {
    match dtype {
        Some(DType::Primitive(ptype, nullability)) => {
            let type_str = match ptype {
                PType::I8 => "byte",
                PType::I16 => "short",
                PType::I32 => "integer",
                PType::I64 => "long",
                PType::F16 => "float",  // Half precision mapped to float
                PType::F32 => "float",
                PType::F64 => "double",
                PType::U8 => "byte",    // Unsigned mapped to signed equivalent
                PType::U16 => "short",
                PType::U32 => "integer",
                PType::U64 => "long",
            };
            (type_str.to_string(), nullability == Nullability::NonNullable)
        }
        Some(DType::Utf8(nullability)) => {
            ("string".to_string(), nullability == Nullability::NonNullable)
        }
        Some(DType::Bool(nullability)) => {
            ("boolean".to_string(), nullability == Nullability::NonNullable)
        }
        Some(DType::Binary(nullability)) => {
            ("binary".to_string(), nullability == Nullability::NonNullable)
        }
        _ => ("string".to_string(), false),
    }
}

/// Helper to get field dtype by index from StructFields
pub fn get_field_dtype_by_index(struct_fields: &StructFields, index: usize) -> Option<DType> {
    // Access field dtype by index - field_by_index returns Option<&DType>
    struct_fields.field_by_index(index).map(|dt| dt.clone())
}

/// Check if Vortex DType is compatible with Vine metadata
pub fn is_compatible_dtype(dtype: &DType) -> bool {
    match dtype {
        DType::Struct(struct_fields, _) => {
            // Check all fields are compatible types
            for i in 0..struct_fields.nfields() {
                if let Some(field_dtype) = struct_fields.field_by_index(i) {
                    let compatible = matches!(
                        field_dtype,
                        DType::Primitive(
                            PType::I8 | PType::I16 | PType::I32 | PType::I64 |
                            PType::F16 | PType::F32 | PType::F64 |
                            PType::U8 | PType::U16 | PType::U32 | PType::U64, _
                        )
                        | DType::Utf8(_)
                        | DType::Bool(_)
                        | DType::Binary(_)
                    );
                    if !compatible {
                        return false;
                    }
                }
            }
            true
        }
        _ => false,
    }
}

/// Get Vortex version info for documentation
pub fn vortex_version() -> &'static str {
    "0.56.0"
}

// ============================================================================
// Phase 2: File I/O Functions
// ============================================================================

/// Create a Vortex session with default settings
///
/// Note: This should be called from within a tokio runtime context
/// (e.g., inside `#[tokio::main]` or `#[tokio::test]`)
pub fn create_session() -> VortexSession {
    // Use VortexSessionDefault trait to get a fully initialized session
    VortexSession::default().with_tokio()
}

/// Write data to a Vortex file
///
/// # Arguments
/// * `path` - Output file path
/// * `metadata` - Vine metadata schema
/// * `rows` - Data rows as CSV-like strings (comma-separated values)
///
/// # Example
/// ```ignore
/// let metadata = Metadata::new("test", vec![
///     MetadataField { id: 1, name: "id".into(), data_type: "integer".into(), is_required: true },
///     MetadataField { id: 2, name: "name".into(), data_type: "string".into(), is_required: false },
/// ]);
/// write_vortex_file("output.vtx", &metadata, &["1,Alice", "2,Bob"]).unwrap();
/// ```
pub fn write_vortex_file<P: AsRef<Path>>(
    path: P,
    metadata: &Metadata,
    rows: &[&str],
) -> VortexResult<u64> {
    let rt = Runtime::new()?;
    rt.block_on(write_vortex_file_async(path, metadata, rows))
}

/// Async implementation of Vortex file writing
pub async fn write_vortex_file_async<P: AsRef<Path>>(
    path: P,
    metadata: &Metadata,
    rows: &[&str],
) -> VortexResult<u64> {
    let session = create_session();

    // Build arrays from rows based on metadata schema
    let array = build_struct_array(metadata, rows)?;

    // Create file and write using async_fs::File which implements VortexWrite
    let file = async_fs::File::create(path.as_ref()).await?;
    let write_options = session.write_options();

    // Convert array to stream and write
    let stream = array.to_array_stream();
    let summary = write_options.write(file, stream).await?;

    Ok(summary.size())
}

/// Build a StructArray from rows based on metadata schema
///
/// **Public** for use by streaming_writer_v2
pub fn build_struct_array(metadata: &Metadata, rows: &[&str]) -> VortexResult<ArrayRef> {
    if metadata.fields.is_empty() {
        return Err("Metadata must have at least one field".into());
    }

    let num_rows = rows.len();
    let mut field_arrays: Vec<ArrayRef> = Vec::with_capacity(metadata.fields.len());
    let mut field_names: Vec<FieldName> = Vec::with_capacity(metadata.fields.len());

    // Parse all rows into column values
    let parsed_rows: Vec<Vec<&str>> = rows
        .iter()
        .map(|row| row.split(',').map(|s| s.trim()).collect())
        .collect();

    for (col_idx, field) in metadata.fields.iter().enumerate() {
        field_names.push(FieldName::from(field.name.clone()));

        let values: Vec<&str> = parsed_rows
            .iter()
            .map(|row| row.get(col_idx).copied().unwrap_or(""))
            .collect();

        let array = build_typed_array(&field.data_type, &values)?;

        field_arrays.push(array);
    }

    // Create struct array
    let struct_array = StructArray::try_new(
        FieldNames::from(field_names),
        field_arrays,
        num_rows,
        Validity::NonNullable,
    )?;

    Ok(struct_array.into_array())
}

/// Build typed array from string values based on Vine type
fn build_typed_array(type_str: &str, values: &[&str]) -> VortexResult<ArrayRef> {
    match type_str.to_lowercase().as_str() {
        // Integer types
        "byte" | "tinyint" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| v.parse::<i8>().unwrap_or(0))
                .collect();
            Ok(array.into_array())
        }
        "short" | "smallint" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| v.parse::<i16>().unwrap_or(0))
                .collect();
            Ok(array.into_array())
        }
        "integer" | "int" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .collect();
            Ok(array.into_array())
        }
        "long" | "bigint" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| v.parse::<i64>().unwrap_or(0))
                .collect();
            Ok(array.into_array())
        }

        // Floating point types
        "float" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| v.parse::<f32>().unwrap_or(0.0))
                .collect();
            Ok(array.into_array())
        }
        "double" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| v.parse::<f64>().unwrap_or(0.0))
                .collect();
            Ok(array.into_array())
        }

        // Boolean
        "boolean" | "bool" => {
            let array: BoolArray = values
                .iter()
                .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"))
                .collect();
            Ok(array.into_array())
        }

        // String
        "string" => {
            let mut builder = VarBinViewBuilder::with_capacity(
                DType::Utf8(Nullability::Nullable),
                values.len(),
            );
            for v in values {
                builder.append_value(v.as_bytes());
            }
            Ok(builder.finish().into_array())
        }

        // Binary (base64 encoded in CSV)
        "binary" => {
            let mut builder = VarBinViewBuilder::with_capacity(
                DType::Binary(Nullability::Nullable),
                values.len(),
            );
            for v in values {
                // Decode base64 or use raw bytes
                let bytes = base64_decode(v).unwrap_or_else(|_| v.as_bytes().to_vec());
                builder.append_value(&bytes);
            }
            Ok(builder.finish().into_array())
        }

        // Date (YYYY-MM-DD format -> days since epoch)
        "date" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| parse_date_to_days(v))
                .collect();
            Ok(array.into_array())
        }

        // Timestamp (ISO format or epoch millis -> milliseconds since epoch)
        "timestamp" => {
            let array: PrimitiveArray = values
                .iter()
                .map(|v| parse_timestamp_to_millis(v))
                .collect();
            Ok(array.into_array())
        }

        // Decimal (stored as string for precision)
        "decimal" => {
            let mut builder = VarBinViewBuilder::with_capacity(
                DType::Utf8(Nullability::Nullable),
                values.len(),
            );
            for v in values {
                builder.append_value(v.as_bytes());
            }
            Ok(builder.finish().into_array())
        }

        other => Err(format!("Unsupported type: {}", other).into()),
    }
}

/// Parse date string (YYYY-MM-DD) to days since Unix epoch
pub fn parse_date_to_days(s: &str) -> i32 {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(|d| (d - epoch).num_days() as i32)
        .unwrap_or(0)
}

/// Parse timestamp string to milliseconds since Unix epoch
pub fn parse_timestamp_to_millis(s: &str) -> i64 {
    use chrono::{DateTime, NaiveDateTime};

    // Try parsing as epoch milliseconds first
    if let Ok(millis) = s.parse::<i64>() {
        return millis;
    }

    // Try ISO 8601 format with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_millis();
    }

    // Try common formats without timezone
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];

    for fmt in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return dt.and_utc().timestamp_millis();
        }
    }

    0
}

/// Base64 decode string using the base64 crate
fn base64_decode(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    BASE64.decode(s.trim()).map_err(|e| e.into())
}

/// Read data from a Vortex file
///
/// # Arguments
/// * `path` - Input file path
///
/// # Returns
/// A tuple of (DType schema, ArrayRef data)
pub fn read_vortex_file<P: AsRef<Path>>(path: P) -> VortexResult<(DType, ArrayRef)> {
    let rt = Runtime::new()?;
    rt.block_on(read_vortex_file_async(path))
}

/// Async implementation of Vortex file reading
pub async fn read_vortex_file_async<P: AsRef<Path>>(path: P) -> VortexResult<(DType, ArrayRef)> {
    let session = create_session();

    // VortexOpenOptions::open accepts types implementing IntoReadSource
    // Path/PathBuf implements this trait
    let vortex_file = session.open_options()
        .open(path.as_ref())
        .await?;

    // Get schema from file
    let dtype = vortex_file.dtype().clone();

    // Read all data using Box::pin for the stream (stream is not Unpin)
    let stream = vortex_file.scan()?.into_array_stream()?;
    let mut pinned_stream = Box::pin(stream);

    let mut arrays: Vec<ArrayRef> = Vec::new();
    while let Some(result) = pinned_stream.as_mut().next().await {
        let array = result?;
        arrays.push(array);
    }

    // For now, return first array (simple case)
    if arrays.is_empty() {
        return Err("No data found in file".into());
    }

    Ok((dtype, arrays.into_iter().next().unwrap()))
}

/// Get row count from an array
pub fn get_row_count(array: &ArrayRef) -> usize {
    array.len()
}

/// Extract column values as strings based on type
fn extract_column_values(child: &ArrayRef, type_str: &str, num_rows: usize) -> Vec<String> {
    use vortex::ToCanonical;

    match type_str.to_lowercase().as_str() {
        // Integer types
        "byte" | "tinyint" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let val: i8 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0);
                    val.to_string()
                })
                .collect()
        }
        "short" | "smallint" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let val: i16 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0);
                    val.to_string()
                })
                .collect()
        }
        "integer" | "int" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let val: i32 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0);
                    val.to_string()
                })
                .collect()
        }
        "long" | "bigint" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let val: i64 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0);
                    val.to_string()
                })
                .collect()
        }

        // Floating point types
        "float" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let val: f32 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0.0);
                    val.to_string()
                })
                .collect()
        }
        "double" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let val: f64 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0.0);
                    val.to_string()
                })
                .collect()
        }

        // Boolean
        "boolean" | "bool" => {
            let bool_arr = child.to_bool();
            (0..num_rows)
                .map(|i| {
                    let val: bool = bool_arr.scalar_at(i).as_ref().try_into().unwrap_or(false);
                    val.to_string()
                })
                .collect()
        }

        // String and Decimal (both stored as UTF8)
        "string" | "decimal" => {
            (0..num_rows)
                .map(|i| {
                    let scalar = child.scalar_at(i);
                    scalar.as_utf8().value().map(|s| s.to_string()).unwrap_or_default()
                })
                .collect()
        }

        // Binary (base64 encode for CSV)
        "binary" => {
            (0..num_rows)
                .map(|i| {
                    let scalar = child.scalar_at(i);
                    if let Some(bytes) = scalar.as_binary().value() {
                        base64_encode(&bytes)
                    } else {
                        String::new()
                    }
                })
                .collect()
        }

        // Date (days since epoch -> YYYY-MM-DD)
        "date" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let days: i32 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0);
                    days_to_date_string(days)
                })
                .collect()
        }

        // Timestamp (millis since epoch -> ISO format)
        "timestamp" => {
            let prim = child.to_primitive();
            (0..num_rows)
                .map(|i| {
                    let millis: i64 = prim.scalar_at(i).as_ref().try_into().unwrap_or(0);
                    millis_to_timestamp_string(millis)
                })
                .collect()
        }

        // Fallback
        _ => (0..num_rows).map(|_| String::new()).collect(),
    }
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

/// Convert milliseconds since epoch to timestamp string
fn millis_to_timestamp_string(millis: i64) -> String {
    use chrono::{TimeZone, Utc};
    if let Some(dt) = Utc.timestamp_millis_opt(millis).single() {
        dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    } else {
        "1970-01-01 00:00:00.000".to_string()
    }
}

/// Simple base64 encode (for binary data in CSV)
fn base64_encode(bytes: &[u8]) -> String {
    const ENCODE_TABLE: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::with_capacity((bytes.len() + 2) / 3 * 4);
    let mut i = 0;

    while i + 2 < bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8) | (bytes[i + 2] as u32);
        result.push(ENCODE_TABLE[((n >> 18) & 0x3F) as usize] as char);
        result.push(ENCODE_TABLE[((n >> 12) & 0x3F) as usize] as char);
        result.push(ENCODE_TABLE[((n >> 6) & 0x3F) as usize] as char);
        result.push(ENCODE_TABLE[(n & 0x3F) as usize] as char);
        i += 3;
    }

    if i + 1 == bytes.len() {
        let n = (bytes[i] as u32) << 16;
        result.push(ENCODE_TABLE[((n >> 18) & 0x3F) as usize] as char);
        result.push(ENCODE_TABLE[((n >> 12) & 0x3F) as usize] as char);
        result.push('=');
        result.push('=');
    } else if i + 2 == bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8);
        result.push(ENCODE_TABLE[((n >> 18) & 0x3F) as usize] as char);
        result.push(ENCODE_TABLE[((n >> 12) & 0x3F) as usize] as char);
        result.push(ENCODE_TABLE[((n >> 6) & 0x3F) as usize] as char);
        result.push('=');
    }

    result
}

/// Convert ArrayRef to CSV-formatted rows for JNI compatibility
///
/// This is the reverse of build_struct_array - extracts data from Vortex arrays
/// and converts back to CSV format for JNI layer.
pub fn array_to_csv_rows(array: &ArrayRef, metadata: &Metadata) -> VortexResult<Vec<String>> {
    use vortex::ToCanonical;

    let struct_array = array.to_struct();
    let num_rows = struct_array.len();
    let mut rows = Vec::with_capacity(num_rows);

    // Extract each column as canonical array for value access
    let mut column_values: Vec<Vec<String>> = Vec::with_capacity(metadata.fields.len());

    // Get all fields from StructArray
    let fields = struct_array.fields();

    for (col_idx, field) in metadata.fields.iter().enumerate() {
        let child = fields.get(col_idx)
            .ok_or_else(|| format!("Missing field at index {}", col_idx))?;

        let values = extract_column_values(child, &field.data_type, num_rows);
        column_values.push(values);
    }

    // Transpose: column-oriented -> row-oriented
    for row_idx in 0..num_rows {
        let row: Vec<String> = column_values.iter()
            .map(|col| col[row_idx].clone())
            .collect();
        rows.push(row.join(","));
    }

    Ok(rows)
}

/// Read all Vortex files from a directory and return CSV rows
///
/// Scans date-partitioned directories (YYYY-MM-DD format) and reads all .vtx files.
/// Returns data as CSV-formatted strings for JNI compatibility.
pub fn read_vine_vortex_data(dir_path: &str) -> VortexResult<Vec<String>> {
    use std::fs;
    use std::path::PathBuf;
    use chrono::NaiveDate;

    let base_path = PathBuf::from(dir_path);

    // Load metadata from vine_meta.json
    let meta_path = base_path.join("vine_meta.json");
    let metadata = Metadata::load(&meta_path)
        .map_err(|e| format!("Failed to load metadata: {}", e))?;

    let mut all_rows = Vec::new();
    let mut directories = Vec::new();

    // Scan for date-partitioned directories
    let dir_entries = fs::read_dir(&base_path)
        .map_err(|e| format!("Cannot read directory {:?}: {}", base_path, e))?;

    for entry_result in dir_entries {
        let entry = entry_result.map_err(|e| format!("Cannot read entry: {}", e))?;
        let path = entry.path();

        if path.is_dir() {
            if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                if let Ok(date) = NaiveDate::parse_from_str(dir_name, "%Y-%m-%d") {
                    directories.push((date, path));
                }
            }
        }
    }

    // Sort directories by date
    directories.sort_by_key(|(date, _)| *date);

    // Read all Vortex files from date directories
    for (_, dir_path) in directories {
        let sub_dir = fs::read_dir(&dir_path)
            .map_err(|e| format!("Cannot read directory {:?}: {}", dir_path, e))?;

        for file_entry_result in sub_dir {
            let file_path = file_entry_result
                .map_err(|e| format!("Cannot read file entry: {}", e))?
                .path();

            // Process .vtx files only
            if file_path.extension().map_or(false, |ext| ext == "vtx") {
                match read_vortex_file(&file_path) {
                    Ok((_, array)) => {
                        match array_to_csv_rows(&array, &metadata) {
                            Ok(rows) => all_rows.extend(rows),
                            Err(e) => eprintln!("Warning: Failed to convert {:?}: {}", file_path, e),
                        }
                    }
                    Err(e) => eprintln!("Warning: Failed to read {:?}: {}", file_path, e),
                }
            }
        }
    }

    Ok(all_rows)
}

/// Write data to Vortex format with date partitioning
///
/// Creates date-partitioned directory structure and writes data as .vtx files.
/// Compatible with existing Vine storage layout.
pub fn write_vine_vortex_data<P: AsRef<Path>>(
    base_path: P,
    rows: &[&str],
) -> VortexResult<u64> {
    use std::fs;
    use chrono::Local;

    let base = base_path.as_ref();

    // Load metadata
    let meta_path = base.join("vine_meta.json");
    let metadata = Metadata::load(&meta_path)
        .map_err(|e| format!("Failed to load metadata: {}", e))?;

    // Create date partition directory
    let date_str = Local::now().format("%Y-%m-%d").to_string();
    let partition_dir = base.join(&date_str);
    fs::create_dir_all(&partition_dir)
        .map_err(|e| format!("Failed to create partition dir: {}", e))?;

    // Generate filename with microsecond precision
    let timestamp = Local::now().format("%H%M%S_%f").to_string();
    let file_path = partition_dir.join(format!("data_{}.vtx", timestamp));

    // Write using existing function
    write_vortex_file(&file_path, &metadata, rows)
}

