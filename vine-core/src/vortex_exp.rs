//! Vortex Experiment Module
//!
//! This module contains experimental code for evaluating Apache Vortex
//! as a potential alternative to Parquet for the Vine datalake format.
//!
//! Enable with: cargo build --features vortex-exp
//!
//! # Testing
//! ```bash
//! cargo test --features vortex-exp vortex_exp
//! ```
//!
//! # Current Status
//! - Phase 1: DType conversion (vine_meta.json <-> Vortex DType) - DONE
//! - Phase 2: File I/O - IN PROGRESS
//! - Phase 3: JNI integration (pending)

#![cfg(feature = "vortex-exp")]

use std::path::Path;

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
/// - "integer" -> PType::I32
/// - "string" -> Utf8
/// - "boolean" -> Bool
/// - "double" -> PType::F64
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

        let dtype = match field.data_type.as_str() {
            "integer" => DType::Primitive(PType::I32, nullability),
            "string" => DType::Utf8(nullability),
            "boolean" => DType::Bool(nullability),
            "double" => DType::Primitive(PType::F64, nullability),
            other => {
                return Err(format!("Unsupported data type: {}", other).into());
            }
        };

        field_types.push(dtype);
    }

    let struct_fields = StructFields::new(FieldNames::from(field_names), field_types.into());

    Ok(DType::Struct(
        struct_fields.into(),
        Nullability::NonNullable,
    ))
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
                    let (data_type, is_required) = match field_dtype {
                        Some(DType::Primitive(ptype, nullability)) => {
                            let type_str = match ptype {
                                PType::I32 | PType::I64 => "integer",
                                PType::F32 | PType::F64 => "double",
                                _ => "string",
                            };
                            (type_str.to_string(), nullability == Nullability::NonNullable)
                        }
                        Some(DType::Utf8(nullability)) => {
                            ("string".to_string(), nullability == Nullability::NonNullable)
                        }
                        Some(DType::Bool(nullability)) => {
                            ("boolean".to_string(), nullability == Nullability::NonNullable)
                        }
                        _ => ("string".to_string(), false),
                    };

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

/// Helper to get field dtype by index from StructFields
fn get_field_dtype_by_index(struct_fields: &StructFields, index: usize) -> Option<DType> {
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
                        DType::Primitive(PType::I32 | PType::I64 | PType::F32 | PType::F64, _)
                            | DType::Utf8(_)
                            | DType::Bool(_)
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
fn build_struct_array(metadata: &Metadata, rows: &[&str]) -> VortexResult<ArrayRef> {
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

        let array = match field.data_type.as_str() {
            "integer" => build_int_array(&values, field.is_required)?,
            "string" => build_string_array(&values, field.is_required)?,
            "boolean" => build_bool_array(&values, field.is_required)?,
            "double" => build_double_array(&values, field.is_required)?,
            other => return Err(format!("Unsupported type: {}", other).into()),
        };

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

fn build_int_array(values: &[&str], _is_required: bool) -> VortexResult<ArrayRef> {
    let array: PrimitiveArray = values
        .iter()
        .map(|v| v.parse::<i32>().unwrap_or(0))
        .collect();
    Ok(array.into_array())
}

fn build_string_array(values: &[&str], _is_required: bool) -> VortexResult<ArrayRef> {
    let mut builder = VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::Nullable), values.len());
    for v in values {
        builder.append_value(v.as_bytes());
    }
    Ok(builder.finish().into_array())
}

fn build_bool_array(values: &[&str], _is_required: bool) -> VortexResult<ArrayRef> {
    let array: BoolArray = values
        .iter()
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"))
        .collect();
    Ok(array.into_array())
}

fn build_double_array(values: &[&str], _is_required: bool) -> VortexResult<ArrayRef> {
    let array: PrimitiveArray = values
        .iter()
        .map(|v| v.parse::<f64>().unwrap_or(0.0))
        .collect();
    Ok(array.into_array())
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_unsupported_type() {
        let metadata = Metadata::new(
            "test",
            vec![MetadataField {
                id: 1,
                name: "unknown".to_string(),
                data_type: "timestamp".to_string(), // Not supported yet
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
}
