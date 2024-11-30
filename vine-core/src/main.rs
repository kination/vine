mod metadata;
mod storage;

use metadata::{Metadata, MetadataField};
use storage::write_data;

fn main() {
    let metadata = Metadata::new("new_table", vec![
        MetadataField { id: 1, name: "id".to_string(), data_type: "i32".to_string(), is_required: true },
        MetadataField { id: 2, name: "name".to_string(), data_type: "String".to_string(), is_required: true }
        ]
    );
    metadata.save("vine_meta.json").expect("Cannot save metadata");

    let sample_data = vec![
        (1, "line"),
        (2, "yahoo"),
        (3, "paypay")
    ];

    write_data("result.parquet", &sample_data).expect("Failed to write data");
}
