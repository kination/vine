//! Vine Metadata Schema Management
//!
//! Handles loading and saving of vine_meta.json schema files.

use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

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

/// Supported Vine data types
///
/// Primitive types:
/// - byte/tinyint: 8-bit signed integer
/// - short/smallint: 16-bit signed integer
/// - integer/int: 32-bit signed integer
/// - long/bigint: 64-bit signed integer
/// - float: 32-bit floating point
/// - double: 64-bit floating point
/// - boolean: true/false
/// - string: UTF-8 text
/// - binary: byte array
///
/// Date/Time types:
/// - date: calendar date (YYYY-MM-DD)
/// - timestamp: date and time with millisecond precision
///
/// Numeric types:
/// - decimal: fixed-precision decimal (precision, scale)
#[derive(Serialize, Deserialize, Clone)]
pub enum Value {
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    String(String),
    Binary(Vec<u8>),
    Date(i32),        // Days since Unix epoch
    Timestamp(i64),   // Milliseconds since Unix epoch
    Decimal(String),  // String representation for precision
}


impl Metadata {
    pub fn new(table_name: &str, fields: Vec<MetadataField>) -> Self {
        Metadata {
            table_name: table_name.to_string(),
            fields: fields
        }
    }

    /// Load metadata from vine_meta.json file
    ///
    /// # Arguments
    /// * `path` - Path to vine_meta.json file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path.as_ref())?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let metadata: Metadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }

    pub fn save(&self, path: &str) -> io::Result<()> {
        let file = File::create(path)?;
        serde_json::to_writer(file, &self)?;
        Ok(())
    }

    // ========================================================================
    // Schema-on-Read Functions
    // ========================================================================

    /// Infer schema from Vortex file in directory
    ///
    /// This enables schema-on-read pattern where metadata is extracted
    /// from Vortex files instead of requiring vine_meta.json.
    ///
    /// # Arguments
    /// * `base_path` - Root directory containing date-partitioned Vortex files
    ///
    /// # Returns
    /// Metadata inferred from Vortex schema
    pub fn infer_from_vortex<P: AsRef<Path>>(base_path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = base_path.as_ref();

        // Find first vortex file, and extract schema
        // Transform Vortex DType to Vine Metadata
        let vortex_file = Self::find_first_vortex_file(base_path)?;
        let (dtype, _) = crate::vortex_exp::read_vortex_file(&vortex_file)
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;
        crate::vortex_exp::dtype_to_metadata(&dtype, "inferred")
            .map_err(|e| -> Box<dyn std::error::Error> { e })
    }

    /// Find the first (or latest) Vortex file in directory tree
    fn find_first_vortex_file(base_path: &Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        if let Ok(entries) = fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "vtx") {
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

        // Sort to get latest date first
        date_dirs.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        for dir_entry in date_dirs {
            let dir_path = dir_entry.path();
            if let Ok(files) = fs::read_dir(&dir_path) {
                // Get the latest vortex file in this directory
                let mut vortex_files: Vec<_> = files
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path()
                            .extension()
                            .map_or(false, |ext| ext == "vtx")
                    })
                    .collect();

                // Sort by name descending (latest file first)
                vortex_files.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

                if let Some(file_entry) = vortex_files.first() {
                    return Ok(file_entry.path());
                }
            }
        }

        Err(format!("No Vortex files found in {:?}", base_path).into())
    }

    /// Update metadata cache asynchronously
    /// This function updates _meta/schema.json in the background without
    /// blocking the write path.
    pub fn update_cache_async<P: AsRef<Path> + Send + 'static>(base_path: P)
    where
        P: Clone,
    {
        let path = base_path.as_ref().to_path_buf();
        std::thread::spawn(move || {
            match Self::infer_from_vortex(&path) {
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
    pub fn save_versioned<P: AsRef<Path>>(
        &self,
        _base_path: P,
        _operation: &str,
    ) -> io::Result<i64> {
        // TODO: Implement versioning (Phase 2)
        Ok(0)
    }

    /// Load specific version of metadata
    pub fn load_version<P: AsRef<Path>>(
        _base_path: P,
        _version: i64,
    ) -> io::Result<Self> {
        // TODO: Implement version loading (Phase 2)
        unimplemented!("Version loading not yet implemented")
    }

    /// Rollback to specific version
    pub fn rollback<P: AsRef<Path>>(
        _base_path: P,
        _target_version: i64,
    ) -> io::Result<()> {
        // TODO: Implement rollback (Phase 2)
        unimplemented!("Rollback not yet implemented")
    }
}
