mod metadata;
mod storage_writer;
mod storage_reader;

use std::ffi::CString;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jobject, jstring};

use metadata::{Metadata, MetadataField};
use storage_writer::write_dynamic_data;
use storage_reader::read_vine_data;

#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_readDataFromVine(
    mut env: JNIEnv,
    class: JClass, 
    dir_path: JString) -> jobject {
    // TODO:
    let path: String = env.get_string(&dir_path).expect("Cannot get data from dir_path").into();
    let rows = read_vine_data(&path);
    let mut result = String::new();

    for row in rows {
        // let row_st
        result.push_str(&row);
        result.push('\n')
    }
    let output = CString::new(result).expect("Cannot generate CString from result");
    
    env.new_string(output.to_str().unwrap()).expect("Cannot create java string").into_raw()
}

#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_writeDataToVine(
    mut env: JNIEnv,
    class: JClass,
    path: JString,data: JString) {
    
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting path").into();
    let rows: Vec<&str> = data_str.lines().collect();
    write_dynamic_data(&path_str, &rows).expect("Failed to write data");
}
