pub mod metadata;
pub mod writer_config;
pub mod writer_cache;
pub mod streaming_writer;
pub mod storage_writer;
pub mod reader_cache;
pub mod storage_reader;

use std::ffi::CString;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jobject, jstring};

use metadata::{Metadata, MetadataField};
use storage_writer::{write_data, VineBatchWriter, VineStreamingWriter};
use storage_reader::read_vine_data;
use writer_config::WriterConfig;
use std::sync::Mutex;
use std::collections::HashMap;

// Global registry for streaming writers
lazy_static::lazy_static! {
    static ref STREAMING_WRITERS: Mutex<HashMap<i64, VineStreamingWriter>> = Mutex::new(HashMap::new());
    static ref WRITER_ID_COUNTER: Mutex<i64> = Mutex::new(0);
}

// ============================================================================
// Reader JNI Functions
// ============================================================================

#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_readDataFromVine(
    mut env: JNIEnv,
    class: JClass,
    dir_path: JString,
) -> jobject {
    let path: String = env
        .get_string(&dir_path)
        .expect("Cannot get data from dir_path")
        .into();
    let rows = read_vine_data(&path);
    let mut result = String::new();

    for row in rows {
        result.push_str(&row);
        result.push('\n')
    }
    let output = CString::new(result).expect("Cannot generate CString from result");

    env.new_string(output.to_str().unwrap())
        .expect("Cannot create java string")
        .into_raw()
}

// ============================================================================
// Batch Writer JNI Functions
// ============================================================================

/// Legacy batch write function (backward compatible)
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_writeDataToVine(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    data: JString,
) {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting data").into();
    let rows: Vec<&str> = data_str.lines().collect();
    write_data(&path_str, &rows).expect("Failed to write data");
}

/// Batch write with balanced configuration
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_batchWriteBalanced(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    data: JString,
) {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting data").into();
    let rows: Vec<&str> = data_str.lines().collect();

    VineBatchWriter::write_balanced(&path_str, &rows).expect("Failed to batch write");
}

/// Batch write with high throughput configuration
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_batchWriteHighThroughput(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    data: JString,
) {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting data").into();
    let rows: Vec<&str> = data_str.lines().collect();

    VineBatchWriter::write_high_throughput(&path_str, &rows)
        .expect("Failed to batch write with high throughput");
}

/// Batch write with high compression configuration
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_batchWriteHighCompression(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    data: JString,
) {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting data").into();
    let rows: Vec<&str> = data_str.lines().collect();

    VineBatchWriter::write_high_compression(&path_str, &rows)
        .expect("Failed to batch write with high compression");
}

// ============================================================================
// Streaming Writer JNI Functions
// ============================================================================

/// Create a new streaming writer and return its ID
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_createStreamingWriter(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    config_type: jni::sys::jint, // 0=balanced, 1=high_throughput, 2=high_compression
) -> jni::sys::jlong {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();

    let writer = match config_type {
        0 => VineStreamingWriter::balanced(&path_str),
        1 => VineStreamingWriter::high_throughput(&path_str),
        2 => VineStreamingWriter::high_compression(&path_str),
        _ => VineStreamingWriter::balanced(&path_str),
    }
    .expect("Failed to create streaming writer");

    let mut counter = WRITER_ID_COUNTER.lock().unwrap();
    let id = *counter;
    *counter += 1;
    drop(counter);

    let mut writers = STREAMING_WRITERS.lock().unwrap();
    writers.insert(id, writer);

    id
}

/// Append batch to existing streaming writer
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_streamingAppendBatch(
    mut env: JNIEnv,
    class: JClass,
    writer_id: jni::sys::jlong,
    data: JString,
) {
    let data_str: String = env.get_string(&data).expect("Fail getting data").into();
    let rows: Vec<&str> = data_str.lines().collect();

    let mut writers = STREAMING_WRITERS.lock().unwrap();
    if let Some(writer) = writers.get_mut(&writer_id) {
        writer.append_batch(&rows).expect("Failed to append batch");
    } else {
        panic!("Writer ID {} not found", writer_id);
    }
}

/// Flush streaming writer
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_streamingFlush(
    mut env: JNIEnv,
    class: JClass,
    writer_id: jni::sys::jlong,
) {
    let mut writers = STREAMING_WRITERS.lock().unwrap();
    if let Some(writer) = writers.get_mut(&writer_id) {
        writer.flush().expect("Failed to flush");
    } else {
        panic!("Writer ID {} not found", writer_id);
    }
}

/// Close and remove streaming writer
#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn Java_io_kination_vine_VineModule_streamingClose(
    mut env: JNIEnv,
    class: JClass,
    writer_id: jni::sys::jlong,
) {
    let mut writers = STREAMING_WRITERS.lock().unwrap();
    if let Some(writer) = writers.remove(&writer_id) {
        writer.close().expect("Failed to close writer");
    } else {
        panic!("Writer ID {} not found", writer_id);
    }
}
