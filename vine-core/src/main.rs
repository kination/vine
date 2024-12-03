mod metadata;
mod storage_writer;
mod storage_reader;

use metadata::{Metadata, MetadataField};
use storage_writer::{write_data, write_dynamic_data};
// use storage_reader::read_data;

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
    
}
