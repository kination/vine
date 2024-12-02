mod metadata;
mod storage_writer;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jobject, jstring};

use metadata::{Metadata, MetadataField};
use storage_writer::write_dynamic_data;

#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_readDataFromVine(env: JNIEnv, class: JClass, path: JString) -> jstring {
    // TODO:
    let sample: JString = env.new_string("sample").unwrap();
    return sample.into_raw()
}

#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_writeDataToVine(mut env: JNIEnv, class: JClass, path: JString, data: JString) {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting path").into();
    let rows: Vec<&str> = data_str.lines().collect();
    write_dynamic_data(&path_str, &rows).expect("Failed to write data");
}
