use parquet::{
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedColumnWriter},
    },
    schema::parser::parse_message_type,
};
use parquet::column::writer::{ColumnWriter};
use parquet::record::{Row, RowAccessor};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use serde_json::from_str; 

// use std::fs;
use std::fs::{File, read_to_string};
use std::path::Path;
use std::sync::Arc;

use crate::{Metadata, MetadataField};

// TODO: deprecated part
pub fn write_data<P: AsRef<Path>>(path: P, data: &Vec<(i32, &str)>) -> parquet::errors::Result<()> {
    let file = File::create(path)?;
    let meta_str = read_to_string("vine_meta.json").expect("Failed to read vine_meta.json");
    let metadata: Metadata = from_str(&meta_str).expect("Failed to deserialize metadata");
    let meta_fields = metadata.fields;

    let mut schema_str = String::from("message schema {\n");
    for field in meta_fields {
        
        // TODO: Apply "required" only when "is_required" are true
        let field_type = match field.data_type.as_str() {
            "i32" => "REQUIRED INT32",
            "String" => "REQUIRED BINARY",
            _ => continue,
        };

        match field_type {
            "String" => schema_str.push_str(&format!("    {} {} (UTF8);\n", field_type, field.name)),
            _ => schema_str.push_str(&format!("    {} {};\n", field_type, field.name))
            
        }
    }
    
    schema_str.push_str("}\n");

    let schema = Arc::new(parse_message_type(schema_str.as_str())?);
    let mut writer = SerializedFileWriter::new(
        file, 
        schema,  
        Arc::new(WriterProperties::builder().build())
    ).unwrap();

    let mut int32_values = Vec::new();
    let mut byte_array_values = Vec::new();

    for row in data {
        int32_values.push(row.0);
        byte_array_values.push(row.1.into());
        // for field in &metadata.fields {
        //     match field.data_type.as_str() {
        //         "String" => {
        //             byte_array_values.push(ByteArray::from(row.1.as_str().unwrap().as_bytes().to_vec()));
        //         },
        //         "i32" => {
        //             int32_values.push(row.1.as_i64().unwrap() as i32); // Assuming row.1 is a serde_json::Value
        //         },
        //         "bool" => {
        //             let bool_value = row.1.as_bool().unwrap();
        //             // Handle the bool value as needed, e.g., store it in a separate vector or process it
        //             // For example, you might want to store it in a Vec<bool> if needed
        //         },
        //         // Handle other types as needed
        //         _ => continue,
        //     }
        // }

    }

    let mut row_group_writer = writer.next_row_group().unwrap();
    
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_writer.typed::<Int32Type>()
                .write_batch(&int32_values, None, None)
                .unwrap();
        col_writer.close().unwrap();
    }

    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_writer.typed::<ByteArrayType>()
                .write_batch(&byte_array_values, None, None)
                .unwrap();
        col_writer.close().unwrap();
    }

    row_group_writer.close().unwrap();
    writer.close().unwrap();

    Ok(())
}

pub fn write_dynamic_data<P: AsRef<Path>>(path: P, data: &Vec<&str>) -> parquet::errors::Result<()> {
    let file = File::create(path)?;
    let meta_str = read_to_string("vine_meta.json").expect("Failed to read vine_meta.json");
    let metadata: Metadata = from_str(&meta_str).expect("Failed to deserialize metadata");
    let meta_fields = metadata.fields.clone();

    let mut schema_str = String::from("message schema {\n");
    for field in meta_fields {
        
        // TODO: Apply "required" only when "is_required" are true
        let field_type = match field.data_type.as_str() {
            "i32" => "REQUIRED INT32",
            "String" => "REQUIRED BINARY",
            _ => continue,
        };

        match field_type {
            "String" => schema_str.push_str(&format!("    {} {} (UTF8);\n", field_type, field.name)),
            _ => schema_str.push_str(&format!("    {} {};\n", field_type, field.name))
            
        }
    }
    
    schema_str.push_str("}\n");

    let schema = Arc::new(parse_message_type(schema_str.as_str())?);
    let mut writer = SerializedFileWriter::new(
        file, 
        schema,  
        Arc::new(WriterProperties::builder().build())
    ).unwrap();

    // let meta_fields = metadata.fields.clone();
    let field_count = metadata.fields.len();
    let mut values: Vec<Vec<ByteArray>> = vec![Vec::new(); field_count];
    let mut int_values: Vec<Vec<i32>> = vec![Vec::new(); field_count];

    for row in data {
        println!("row -> {:?}", row);
        let values_array: Vec<&str> = row.split(',')
            .map(|s| s.trim())
            .collect();
        
        for (i, field) in metadata.fields.iter().enumerate() {
            let raw_value = values_array.get(i).unwrap_or(&"");
            
            match field.data_type.as_str() {
                "String" => {
                    values[i].push(ByteArray::from(*raw_value));
                },
                "i32" => {
                    let int_value = raw_value.parse::<i32>().unwrap_or_default();
                    int_values[i].push(int_value);
                },
                _ => continue,
            }
        }
    }

    let mut row_group_writer = writer.next_row_group().unwrap();
    
    for (i, field) in metadata.fields.iter().enumerate() {
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match field.data_type.as_str() {
                "String" => {
                    col_writer.typed::<ByteArrayType>()
                            .write_batch(&values[i], None, None)
                            .unwrap();
                },
                "i32" => {
                    col_writer.typed::<Int32Type>()
                            .write_batch(&int_values[i], None, None)
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

