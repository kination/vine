use vine_core::writer_config::WriterConfig;

#[test]
fn test_writer_config_default() {
    let config = WriterConfig::default();

    assert_eq!(config.max_rows_per_file, 100_000);
}

#[test]
fn test_writer_config_with_max_rows() {
    let config = WriterConfig::with_max_rows(50_000);

    assert_eq!(config.max_rows_per_file, 50_000);
}

#[test]
fn test_writer_config_with_max_rows_small() {
    let config = WriterConfig::with_max_rows(100);

    assert_eq!(config.max_rows_per_file, 100);
}

#[test]
fn test_writer_config_with_max_rows_large() {
    let config = WriterConfig::with_max_rows(10_000_000);

    assert_eq!(config.max_rows_per_file, 10_000_000);
}

#[test]
fn test_writer_config_clone() {
    let original = WriterConfig::with_max_rows(75_000);
    let cloned = original.clone();

    assert_eq!(original.max_rows_per_file, cloned.max_rows_per_file);
}

#[test]
fn test_writer_config_debug() {
    let config = WriterConfig::with_max_rows(25_000);
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("WriterConfig"));
    assert!(debug_str.contains("25000"));
}
