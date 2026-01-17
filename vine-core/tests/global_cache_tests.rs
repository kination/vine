use vine_core::global_cache::{invalidate_all_caches, invalidate_reader_cache, invalidate_writer_cache};

#[test]
fn test_cache_invalidation() {
    // Just verify invalidation doesn't panic on non-existent keys
    invalidate_reader_cache("/non/existent/path");
    invalidate_writer_cache("/non/existent/path");
    invalidate_all_caches("/non/existent/path");
}
