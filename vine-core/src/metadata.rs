use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self};
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::basic::Type as PhysicalType;

#[derive(Serialize, Deserialize, Clone)]
pub struct Metadata {
    pub table_name: String,
    pub fields: Vec<MetadataField>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MetadataField {
    pub id: i32,
    pub name: String,
    pub data_type: String,
    pub is_required: bool
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Value {
    String(String),
    Int(i32),
    Bool(bool),
    Double(f64),
}


impl Metadata {
    pub fn new(table_name: &str, fields: Vec<MetadataField>) -> Self {
        Metadata {
            table_name: table_name.to_string(),
            fields: fields
        }
    }

    pub fn save(&self, path: &str) -> io::Result<()> {
        let file = File::create(path)?;
        serde_json::to_writer(file, &self)?;
        Ok(())
    }

    // ========================================================================
    // Schema-on-Read Functions
    // ========================================================================

    /// Infer schema from Parquet file in directory
    ///
    /// This enables schema-on-read pattern where metadata is extracted
    /// from Parquet files instead of requiring vine_meta.json.
    ///
    /// # Arguments
    /// * `base_path` - Root directory containing date-partitioned Parquet files
    ///
    /// # Returns
    /// Metadata inferred from Parquet schema
    pub fn infer_from_parquet<P: AsRef<Path>>(base_path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = base_path.as_ref();

        // Find first parquet file (scan date directories)
        let parquet_file = Self::find_first_parquet_file(base_path)?;

        // Read Parquet metadata
        let file = File::open(&parquet_file)
            .map_err(|e| format!("Failed to open parquet file {:?}: {}", parquet_file, e))?;
        let reader = SerializedFileReader::new(file)
            .map_err(|e| format!("Failed to read parquet file: {}", e))?;

        let parquet_metadata = reader.metadata();
        let schema = parquet_metadata.file_metadata().schema();

        // Convert Parquet schema to Vine Metadata
        let fields: Vec<MetadataField> = schema
            .get_fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let data_type = Self::map_parquet_type_to_vine(field.get_physical_type());
                MetadataField {
                    id: (i + 1) as i32,
                    name: field.name().to_string(),
                    data_type,
                    is_required: !field.is_optional(),
                }
            })
            .collect();

        if fields.is_empty() {
            return Err("No fields found in Parquet schema".into());
        }

        Ok(Metadata {
            table_name: "inferred".to_string(),
            fields,
        })
    }

    /// Find the first (or latest) Parquet file in directory tree
    fn find_first_parquet_file(base_path: &Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        // First, check for direct .parquet files in base_path
        if let Ok(entries) = fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "parquet") {
                    return Ok(path);
                }
            }
        }

        // Then check date-partitioned directories (YYYY-MM-DD format)
        // Sort in reverse to get the latest first
        let mut date_dirs: Vec<_> = fs::read_dir(base_path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter(|e| {
                // Check if directory name matches date pattern
                e.file_name()
                    .to_str()
                    .map_or(false, |name| name.len() == 10 && name.chars().nth(4) == Some('-'))
            })
            .collect();

        // Sort by name descending (latest date first)
        date_dirs.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        for dir_entry in date_dirs {
            let dir_path = dir_entry.path();
            if let Ok(files) = fs::read_dir(&dir_path) {
                // Get the latest parquet file in this directory
                let mut parquet_files: Vec<_> = files
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path()
                            .extension()
                            .map_or(false, |ext| ext == "parquet")
                    })
                    .collect();

                // Sort by name descending (latest file first)
                parquet_files.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

                if let Some(file_entry) = parquet_files.first() {
                    return Ok(file_entry.path());
                }
            }
        }

        Err(format!("No Parquet files found in {:?}", base_path).into())
    }

    /// Map Parquet physical type to Vine data type string
    fn map_parquet_type_to_vine(physical_type: PhysicalType) -> String {
        match physical_type {
            PhysicalType::INT32 => "integer".to_string(),
            PhysicalType::INT64 => "integer".to_string(),  // Map to integer for now
            PhysicalType::BOOLEAN => "boolean".to_string(),
            PhysicalType::FLOAT => "double".to_string(),
            PhysicalType::DOUBLE => "double".to_string(),
            PhysicalType::BYTE_ARRAY => "string".to_string(),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => "string".to_string(),
            PhysicalType::INT96 => "string".to_string(),  // Timestamp, treat as string for now
        }
    }

    /// Update metadata cache asynchronously
    ///
    /// This function updates _meta/schema.json in the background without
    /// blocking the write path.
    ///
    /// # Arguments
    /// * `base_path` - Root directory
    pub fn update_cache_async<P: AsRef<Path> + Send + 'static>(base_path: P)
    where
        P: Clone,
    {
        let path = base_path.as_ref().to_path_buf();
        std::thread::spawn(move || {
            match Self::infer_from_parquet(&path) {
                Ok(metadata) => {
                    if let Err(e) = metadata.save_to_cache(&path) {
                        eprintln!("Warning: Failed to save metadata cache: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to infer schema for cache: {}", e);
                }
            }
        });
    }

    /// Load cached schema from _meta/schema.json
    ///
    /// # Arguments
    /// * `base_path` - Root directory
    ///
    /// # Returns
    /// Cached metadata if exists, None otherwise
    pub fn load_cached<P: AsRef<Path>>(base_path: P) -> Option<Self> {
        let cache_path = base_path.as_ref().join("_meta").join("schema.json");
        if cache_path.exists() {
            let content = fs::read_to_string(&cache_path).ok()?;
            serde_json::from_str(&content).ok()
        } else {
            None
        }
    }

    /// Save metadata to cache location (_meta/schema.json)
    ///
    /// # Arguments
    /// * `base_path` - Root directory
    pub fn save_to_cache<P: AsRef<Path>>(&self, base_path: P) -> io::Result<()> {
        let cache_dir = base_path.as_ref().join("_meta");
        fs::create_dir_all(&cache_dir)?;
        let cache_path = cache_dir.join("schema.json");
        self.save(cache_path.to_str().unwrap())
    }

    // ========================================================================
    // Versioning Functions (Skeleton - Future Phase)
    // ========================================================================

    /// Save metadata with version tracking
    ///
    /// Creates a new version entry in _meta/versions/ directory.
    ///
    /// # Arguments
    /// * `base_path` - Root directory
    /// * `operation` - Operation type (e.g., "CREATE", "ADD_COLUMN", "ROLLBACK")
    ///
    /// # Returns
    /// New version number
    ///
    /// # Implementation TODO (Phase 2)
    /// 1. Create _meta/versions/ directory
    /// 2. Determine next version number
    /// 3. Create version file with metadata + operation info
    /// 4. Update vine_meta.json as cache
    pub fn save_versioned<P: AsRef<Path>>(
        &self,
        _base_path: P,
        _operation: &str,
    ) -> io::Result<i64> {
        // TODO: Implement versioning (Phase 2)
        // Future: Delta Lake-style transaction log
        Ok(0)
    }

    /// Load specific version of metadata
    ///
    /// # Arguments
    /// * `base_path` - Root directory
    /// * `version` - Version number to load
    ///
    /// # Implementation TODO (Phase 2)
    /// 1. Read _meta/versions/{version:020}.json
    /// 2. Parse and return metadata
    pub fn load_version<P: AsRef<Path>>(
        _base_path: P,
        _version: i64,
    ) -> io::Result<Self> {
        // TODO: Implement version loading (Phase 2)
        unimplemented!("Version loading not yet implemented")
    }

    /// Rollback to specific version
    ///
    /// # Arguments
    /// * `base_path` - Root directory
    /// * `target_version` - Version to rollback to
    ///
    /// # Implementation TODO (Phase 2)
    /// 1. Load target version metadata
    /// 2. Update vine_meta.json
    /// 3. Create new version entry with operation="ROLLBACK"
    pub fn rollback<P: AsRef<Path>>(
        _base_path: P,
        _target_version: i64,
    ) -> io::Result<()> {
        // TODO: Implement rollback (Phase 2)
        unimplemented!("Rollback not yet implemented")
    }
}
