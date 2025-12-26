use crate::metadata::Metadata;
use parquet::schema::types::Type;
use parquet::schema::parser::parse_message_type;
use std::sync::Arc;
use std::path::PathBuf;

/// Cached schema and metadata to avoid repeated parsing
pub struct WriterCache {
    pub metadata: Metadata,
    pub schema: Arc<Type>,
    pub base_path: PathBuf,
}

impl WriterCache {
    /// Create new cache by loading metadata from path
    pub fn new(base_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let meta_path = base_path.join("vine_meta.json");
        let meta_str = std::fs::read_to_string(&meta_path)
            .map_err(|e| format!("Failed to read metadata from {:?}: {}", meta_path, e))?;

        let metadata: Metadata = serde_json::from_str(&meta_str)
            .map_err(|e| format!("Failed to parse metadata: {}", e))?;

        let schema = Self::build_schema(&metadata)?;

        Ok(Self {
            metadata,
            schema,
            base_path,
        })
    }

    /// Build Parquet schema from metadata
    fn build_schema(metadata: &Metadata) -> Result<Arc<Type>, Box<dyn std::error::Error>> {
        let mut schema_str = String::from("message schema {\n");

        for field in &metadata.fields {
            let field_type = match field.data_type.as_str() {
                "integer" => "REQUIRED INT32",
                "string" => "REQUIRED BINARY",
                "boolean" => "REQUIRED BOOLEAN",
                "double" => "REQUIRED DOUBLE",
                _ => continue,
            };

            match field_type {
                "REQUIRED BINARY" => {
                    schema_str.push_str(&format!("    {} {} (UTF8);\n", field_type, field.name))
                }
                _ => schema_str.push_str(&format!("    {} {};\n", field_type, field.name))
            }
        }

        schema_str.push_str("}\n");

        Ok(Arc::new(parse_message_type(&schema_str)?))
    }

    /// Reload metadata and schema (useful if schema changes)
    pub fn reload(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let meta_path = self.base_path.join("vine_meta.json");
        let meta_str = std::fs::read_to_string(&meta_path)
            .map_err(|e| format!("Failed to read metadata from {:?}: {}", meta_path, e))?;

        self.metadata = serde_json::from_str(&meta_str)
            .map_err(|e| format!("Failed to parse metadata: {}", e))?;

        self.schema = Self::build_schema(&self.metadata)?;

        Ok(())
    }
}
