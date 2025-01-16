

use std::fs::{self, File, read_to_string};
use chrono::NaiveDate;
use serde_json::Value;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;


pub fn read_vine_data(dir_path: &str) -> Vec<String> {
    let mut row_list = Vec::new();
    let mut directories = Vec::new();
    let dir_entries = fs::read_dir(dir_path).expect("Cannot find dir_path");

    for e in dir_entries {
        let entry = e.expect("Cannot read entry");
        let path = entry.path();
        if path.is_dir() {
            let dir_data_str = path.file_name().and_then(|s| s.to_str()).expect("cannot parse");
            if let Ok(date) = NaiveDate::parse_from_str(dir_data_str, "%Y-%m-%d") {
                directories.push((date, path));
            }
        }
    }

    directories.sort_by_key(|(date, _)| *date);

    // Read metadata file
    let meta_str = read_to_string("vine-test/vine_meta.json").expect("Failed to read vine_meta.json");
    let meta: Value = serde_json::from_str(&meta_str)
                    .expect("Failed to parse metadata JSON");

    for (_, path) in directories {
        let sub_entries = fs::read_dir(path).expect("Cannot read path");
        for se in sub_entries {
            let file_path = se.expect("Cannot get sub entry").path();

            // TODO: accept other types, not only parquet
            if file_path.extension().map_or(false, |ext| ext == "parquet") {
                // println!("Reading file: {:?}", file_path);
                
                let fields = meta["fields"].as_array()
                    .expect("fields should be an array");

                let file = File::open(file_path).expect("Cannot open file from file_path");
                let reader = SerializedFileReader::new(file).expect("cannot serialize file");
                let iter = reader.get_row_iter(None).expect("Cannot get row iterator");

                for row_result in iter {
                    if let Ok(row) = row_result {
                        let mut values = Vec::new();
                        
                        for field in fields {
                            // Fields are 1-indexed in metadata but 0-indexed in parquet
                            let col_index = (field["id"].as_i64().unwrap_or(0) - 1) as usize;
                            let data_type = field["data_type"].as_str().expect("data_type should be string");

                            let value = match data_type {
                                "i32" => row.get_int(col_index).unwrap_or_default().to_string(),
                                "String" => {
                                    // let bytes = row.get_bytes(col_index).expect("Error on getting bytes");
                                    // String::from_utf8(bytes.data().to_vec()).unwrap_or_default()
                                    row.get_string(col_index).unwrap().clone()
                                },
                                _ => String::from(""),
                            };
                            values.push(value);
                        }
                        
                        row_list.push(values.join(","));
                    }
                }
            }
        }
    }

    row_list
}
