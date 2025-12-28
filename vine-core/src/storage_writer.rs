use std::path::Path;

use crate::vine_batch_writer::VineBatchWriter;

/// Write data to Vine storage
pub fn write_data<P: AsRef<Path>>(path: P, data: &Vec<&str>) -> Result<(), Box<dyn std::error::Error>> {
    VineBatchWriter::write(path, data)
}
