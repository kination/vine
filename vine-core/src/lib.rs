mod metadata;
mod storage_writer;

use std::os::raw::c_char;
use std::ffi::CStr;
use metadata::{Metadata, MetadataField};
use storage_writer::{write_data, write_dynamic_data};

#[no_mangle]
pub extern "C" fn read_data_from_vine(path: *const c_char) {
    // TODO:
}

#[no_mangle]
pub extern "C" fn write_data_to_vine(path: *const c_char, data: *const c_char){
    let path_str = unsafe { 
        CStr::from_ptr(path).to_string_lossy().into_owned() 
    }; 
    let data_str = unsafe { 
        CStr::from_ptr(data).to_string_lossy().into_owned() 
    };
    let rows: Vec<&str> = data_str.lines().collect();
    write_dynamic_data(&path_str, &rows).expect("Failed to write data");
}
