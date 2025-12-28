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
//! - Phase 2: File I/O (pending - requires stable Vortex API)
//! - Phase 3: JNI integration (pending)

#![cfg(feature = "vortex-exp")]

use vortex_dtype::{DType, FieldName, FieldNames, Nullability, PType, StructFields};

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
}
