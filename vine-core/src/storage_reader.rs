

use std::fs::{self, File};
use chrono::NaiveDate;
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

    for (_, path) in directories {
        let sub_entries = fs::read_dir(path).expect("Cannot read path");
        for se in sub_entries {
            let file_path = se.expect("Cannot get sub entry").path();

            // TODO: accept other types, not only parquet
            if file_path.extension().map_or(false, |ext| ext == "parquet") {
                let file = File::open(file_path).expect("Cannot open file from file_path");
                let reader = SerializedFileReader::new(file).expect("cannot serialize file");
                let iter = reader.get_row_iter(None).expect("Cannot get row iterator");

                for row_result in iter {
                    if let Ok(row) = row_result {
                        let id = row.get_int(0).unwrap_or_default();
                        let name_bytes = row.get_bytes(1).expect("Error getting byte data");
                        let name = String::from_utf8(name_bytes.data().to_vec()).unwrap_or_default();
                        row_list.push(format!("{},{}", id, name));
                    }
                }
            }
        }
    }

    row_list
}
