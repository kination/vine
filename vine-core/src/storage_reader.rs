

use std::fs::{self, File};
use std::path::Path;
use std::io;
use parquet::file::reader::{FileReader, SerializedFileReader};
// use arrow::record_batch::RecordBatch;
// use arrow::array::ArrayRef;

pub fn read_data<P: AsRef<Path>>(path: P) -> io::Result<Vec<String>> {
    let mut all_batches = Vec::new();
    
    // Read directory entries
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        
        // TODO: support other types later
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            println!("No parquet file in {:?}", path);
            continue;
        }

        // Open parquet file
        let file = File::open(&path)?;
        let reader = SerializedFileReader::new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Read all record batches from the file
        let parquet_reader = reader.get_row_iter(None)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        for row in parquet_reader {
            let row = row.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let row_string = row.get_column_iter()
                .map(|(key, value)| value.to_string())
                .collect::<Vec<_>>()
                .join(",");
            all_batches.push(row_string);
        }
    }

    println!("Result -> {:?}", all_batches);
    
    Ok(all_batches)
}


// fn rows_to_record_batch(
//     schema: &arrow::datatypes::Schema,
//     rows: Vec<arrow::array::ArrayRef>,
// ) -> Result<RecordBatch, arrow::error::ArrowError> {
//     RecordBatch::try_new(
//         std::sync::Arc::new(schema.clone()),
//         rows,
//     )
// }