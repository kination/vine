mod metadata;
mod storage_writer;
mod storage_reader;

use metadata::{Metadata, MetadataField};
use storage_writer::{write_data, write_dynamic_data};
use storage_reader::read_data;

#[derive(Debug)]
struct Record {
    id: i32,
    name: String,
}

fn main() {
    let metadata = Metadata::new("new_table", vec![
        MetadataField { id: 1, name: "id".to_string(), data_type: "i32".to_string(), is_required: true },
        MetadataField { id: 2, name: "name".to_string(), data_type: "String".to_string(), is_required: true }
        ]
    );
    metadata.save("vine_meta.json").expect("Cannot save metadata");

    /*
    let sample_data = vec![
        (1, "line"),
        (2, "yahoo"),
        (3, "paypay")
    ];

    write_data("result.parquet", &sample_data).expect("Failed to write data");
    */

    // let sample_dynamic_data = vec![
    //     "1,line",
    //     "2,yahoo",
    //     "3,paypay"
    // ];
    // write_dynamic_data("result_dy", &sample_dynamic_data).expect("Failed to write data");

    let read_result = read_data("result_dy").expect("Failed reading data");
    let records: Vec<Record> = read_result
        .iter()
        .filter_map(|row| {
            let parts: Vec<&str> = row.split(',').collect();
            println!("parts -> {:?}", parts);
            if parts.len() != 2 {
                return None;
            }
            
            let id = parts[0].parse::<i32>().ok()?;
            // Convert Vec<u8> to String
            let name_bytes = parts[1].trim_matches(|c| c == '[' || c == ']')
                .split(',')
                .filter_map(|s| s.trim().parse::<u8>().ok())
                .collect::<Vec<u8>>();
            let name = String::from_utf8(name_bytes).ok()?;
            
            Some(Record { id, name })
        })
        .collect();
    
    println!("{:?}", records);
    
}
