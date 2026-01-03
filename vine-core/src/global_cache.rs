//! Global cache management for Vine
//!
//! Provides internal cache management so external modules (vine-spark, vine-trino)
//! don't need to manage caching themselves.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::metadata::Metadata;
use crate::reader_cache::ReaderCache;
use crate::writer_cache::WriterCache;

lazy_static::lazy_static! {
    /// Global reader cache: path -> ReaderCache
    static ref READER_CACHE: Mutex<HashMap<String, ReaderCache>> = Mutex::new(HashMap::new());

    /// Global writer cache: path -> WriterCache
    static ref WRITER_CACHE: Mutex<HashMap<String, WriterCache>> = Mutex::new(HashMap::new());
}

// ============================================================================
// Reader Cache Functions
// ============================================================================

/// Get cached metadata for reading, or create cache if not exists
///
/// This function handles the caching logic internally:
/// 1. Check if cache exists for path
/// 2. If not, create cache (with fallback to infer from Vortex)
/// 3. Return cloned metadata
///
/// External modules just call this function without worrying about cache management.
pub fn get_reader_metadata(path: &str) -> Result<Metadata, Box<dyn std::error::Error>> {
    let path_buf = PathBuf::from(path);
    let cache_key = path.to_string();
    let mut cache_map = READER_CACHE.lock().unwrap();

    // Return cached metadata if exists
    if let Some(cache) = cache_map.get(&cache_key) {
        return Ok(cache.metadata.clone());
    }

    // Cache miss - create new cache
    let cache = ReaderCache::new_with_fallback(path_buf)?;
    let metadata = cache.metadata.clone();
    cache_map.insert(cache_key, cache);

    Ok(metadata)
}

/// Get reader cache reference for reading operations
///
/// Returns a cloned ReaderCache to avoid lock contention.
pub fn get_reader_cache(path: &str) -> Result<ReaderCache, Box<dyn std::error::Error>> {
    let path_buf = PathBuf::from(path);
    let cache_key = path.to_string();

    let mut cache_map = READER_CACHE.lock().unwrap();

    if let Some(cache) = cache_map.get(&cache_key) {
        // Clone the cache to return
        return Ok(ReaderCache {
            metadata: cache.metadata.clone(),
            base_path: cache.base_path.clone(),
        });
    }

    // Cache miss - create new cache with fallback
    let cache = ReaderCache::new_with_fallback(path_buf)?;
    let result = ReaderCache {
        metadata: cache.metadata.clone(),
        base_path: cache.base_path.clone(),
    };
    cache_map.insert(cache_key, cache);

    Ok(result)
}

/// Invalidate reader cache for a path
///
/// Call this if metadata has changed and cache needs to be refreshed.
pub fn invalidate_reader_cache(path: &str) {
    let mut cache_map = READER_CACHE.lock().unwrap();
    cache_map.remove(path);
}

// ============================================================================
// Writer Cache Functions
// ============================================================================

/// Get cached metadata for writing, or create cache if not exists
///
/// This function handles the caching logic internally:
/// 1. Check if cache exists for path
/// 2. If not, load from vine_meta.json
/// 3. Return cloned metadata
///
/// External modules just call this function without worrying about cache management.
pub fn get_writer_metadata(path: &str) -> Result<Metadata, Box<dyn std::error::Error>> {
    let path_buf = PathBuf::from(path);
    let cache_key = path.to_string();

    let mut cache_map = WRITER_CACHE.lock().unwrap();

    if let Some(cache) = cache_map.get(&cache_key) {
        return Ok(cache.metadata.clone());
    }

    // Cache miss - create new cache
    let cache = WriterCache::new(path_buf)?;
    let metadata = cache.metadata.clone();
    cache_map.insert(cache_key, cache);

    Ok(metadata)
}

/// Get writer cache reference for writing operations
///
/// Returns a cloned WriterCache to avoid lock contention.
pub fn get_writer_cache(path: &str) -> Result<WriterCache, Box<dyn std::error::Error>> {
    let path_buf = PathBuf::from(path);
    let cache_key = path.to_string();

    let mut cache_map = WRITER_CACHE.lock().unwrap();

    if let Some(cache) = cache_map.get(&cache_key) {
        // Clone the cache to return
        return Ok(WriterCache {
            metadata: cache.metadata.clone(),
            base_path: cache.base_path.clone(),
        });
    }

    // Cache miss - create new cache
    let cache = WriterCache::new(path_buf)?;
    let result = WriterCache {
        metadata: cache.metadata.clone(),
        base_path: cache.base_path.clone(),
    };
    cache_map.insert(cache_key, cache);

    Ok(result)
}

/// Invalidate writer cache for a path
///
/// Call this if metadata has changed and cache needs to be refreshed.
pub fn invalidate_writer_cache(path: &str) {
    let mut cache_map = WRITER_CACHE.lock().unwrap();
    cache_map.remove(path);
}

/// Invalidate all caches for a path (both reader and writer)
pub fn invalidate_all_caches(path: &str) {
    invalidate_reader_cache(path);
    invalidate_writer_cache(path);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_invalidation() {
        // Just verify invalidation doesn't panic on non-existent keys
        invalidate_reader_cache("/non/existent/path");
        invalidate_writer_cache("/non/existent/path");
        invalidate_all_caches("/non/existent/path");
    }
}
