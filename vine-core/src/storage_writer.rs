use parquet::{
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedColumnWriter},
    },
    schema::parser::parse_message_type,
};
use parquet::column::writer::{ColumnWriter};
use parquet::record::{Row, RowAccessor};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type, BoolType, DoubleType};
use chrono::Local;

use std::fs::{self, File, read_to_string};
use std::path::Path;
use std::sync::Arc;

use crate::metadata::{Metadata, MetadataField, Value};


pub fn write_data<P: AsRef<Path>>(path: P, data: &Vec<&str>) -> parquet::errors::Result<()> {
    let now = Local::now();
    let date_dir = now.format("%Y-%m-%d").to_string();
    let time_str = now.format("%H%M%S").to_string();

    let base_path = path.as_ref();
    let date_path = base_path.join(&date_dir);

    fs::create_dir_all(&date_path)?;

    let file_name = format!("data_{}.parquet", time_str);
    let file_path = date_path.join(file_name);
    let file = File::create(file_path)?;

    // Read metadata from root of output directory
    let meta_path = base_path.join("vine_meta.json");
    let meta_str = fs::read_to_string(&meta_path)
        .unwrap_or_else(|_| panic!("Failed to read metadata from {:?}", meta_path));
    let metadata: Metadata = serde_json::from_str(&meta_str).expect("Failed to deserialize metadata");
    let meta_fields = metadata.fields.clone();

    let mut schema_str = String::from("message schema {\n");
    for field in meta_fields {
        let field_type = match field.data_type.as_str() {
            "integer" => "REQUIRED INT32",
            "string" => "REQUIRED BINARY",
            "boolean" => "REQUIRED BOOLEAN",
            "double" => "REQUIRED DOUBLE",
            _ => continue,
        };

        match field_type {
            "REQUIRED BINARY" => schema_str.push_str(&format!("    {} {} (UTF8);\n", field_type, field.name)),
            _ => schema_str.push_str(&format!("    {} {};\n", field_type, field.name))
        }
    }
    
    schema_str.push_str("}\n");

    let schema = Arc::new(parse_message_type(schema_str.as_str())?);
    let props = WriterProperties::builder()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
        .build();
    let mut writer = SerializedFileWriter::new(
        file, 
        schema,  
        Arc::new(props)
    ).unwrap();

    let field_count = metadata.fields.len();
    let mut values: Vec<Vec<Value>> = vec![Vec::new(); field_count];

    for row in data {
        let values_array: Vec<&str> = row.split(',')
            .map(|s| s.trim())
            .collect();
        
        for (i, field) in metadata.fields.iter().enumerate() {
            let raw_value = values_array.get(i).unwrap_or(&"");
            
            match field.data_type.as_str() {
                "string" => {
                    values[i].push(Value::String(raw_value.to_string()));
                },
                "integer" => {
                    let int_value = raw_value.parse::<i32>().unwrap_or_default();
                    values[i].push(Value::Int(int_value));
                },
                "boolean" => {
                    let bool_value = raw_value.parse::<bool>().unwrap_or_default();
                    values[i].push(Value::Bool(bool_value));
                },
                "double" => {
                    let double_value = raw_value.parse::<f64>().unwrap_or_default();
                    values[i].push(Value::Double(double_value));
                },
                _ => continue,
            }
        }
    }

    let mut row_group_writer = writer.next_row_group().unwrap();
    
    for (i, field) in metadata.fields.iter().enumerate() {
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match field.data_type.as_str() {
                "string" => {
                    let string_values: Vec<ByteArray> = values[i].iter()
                        .filter_map(|v| {
                            if let Value::String(s) = v {
                                Some(ByteArray::from(s.as_str()))
                            } else {
                                None
                            }
                         })
                        .collect();
                    col_writer.typed::<ByteArrayType>()
                        .write_batch(&string_values, None, None)
                        .unwrap();
                },
                "integer" => {
                    let int_values: Vec<i32> = values[i].iter()
                        .filter_map(|v| {
                            if let Value::Int(i) = v {
                                Some(*i)
                            } else {
                                None
                            }
                        })
                        .collect();
                    col_writer.typed::<Int32Type>()
                        .write_batch(&int_values, None, None)
                        .unwrap();
                },
                "boolean" => {
                    let bool_values: Vec<bool> = values[i].iter()
                        .filter_map(|v| {
                            if let Value::Bool(b) = v {
                                Some(*b)
                            } else {
                                None
                            }
                        })
                        .collect();
                    col_writer.typed::<BoolType>()
                        .write_batch(&bool_values, None, None)
                        .unwrap();
                },
                "double" => {
                    let double_values: Vec<f64> = values[i].iter()
                        .filter_map(|v| {
                            if let Value::Double(d) = v {
                                Some(*d)
                            } else {
                                None
                            }
                        })
                        .collect();
                    col_writer.typed::<DoubleType>()
                        .write_batch(&double_values, None, None)
                        .unwrap();
                },
                _ => continue,
            }
            col_writer.close().unwrap();
        }
    }

    row_group_writer.close().unwrap();
    writer.close().unwrap();

    Ok(())
}
