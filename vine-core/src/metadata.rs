use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, Write};

#[derive(Serialize, Deserialize, Clone)]
pub struct Metadata {
    pub table_name: String,
    pub fields: Vec<MetadataField>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MetadataField {
    pub id: i32,
    pub name: String,
    pub data_type: String,
    pub is_required: bool
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Value {
    String(String),
    Int(i32),
    Bool(bool),
    Double(f64),
}


impl Metadata {
    pub fn new(table_name: &str, fields: Vec<MetadataField>) -> Self {
        Metadata {
            table_name: table_name.to_string(),
            fields: fields
        }
    }

    pub fn save(&self, path: &str) -> io::Result<()> {
        let file = File::create(path)?;
        serde_json::to_writer(file, &self)?;
        Ok(())
    }
}
