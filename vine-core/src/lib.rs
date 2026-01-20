pub mod metadata;
pub mod writer_config;
pub mod writer_cache;
pub mod streaming_writer;
pub mod streaming_writer_v2;
pub mod vine_batch_writer;
pub mod vine_streaming_writer;
pub mod storage_writer;
pub mod reader_cache;
pub mod storage_reader;
pub mod global_cache;
pub mod vortex_exp;
pub mod arrow_bridge;

use std::ffi::CString;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jobject;

use storage_writer::write_data;
use vine_batch_writer::VineBatchWriter;
use vine_streaming_writer::VineStreamingWriter;
use storage_reader::read_vine_data;
use std::sync::Mutex;
use std::collections::HashMap;

// Global streaming writer registry for JNI handle tracking
lazy_static::lazy_static! {
    static ref STREAMING_WRITERS: Mutex<HashMap<i64, VineStreamingWriter>> = Mutex::new(HashMap::new());
    static ref WRITER_ID_COUNTER: Mutex<i64> = Mutex::new(0);
}

// ============================================================================
// Reader JNI Functions
// ============================================================================

/// Read data from Vine storage
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
        .expect("Cannot find data in 'dir_path'")
        .into();

    let rows: Vec<String> = read_vine_data(&path);
    let mut result: String = String::new();

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

/// Write data to Vine storage
///
/// # Deprecated
/// This function uses CSV string format which is inefficient.
/// Use `Java_io_kination_vine_VineModule_batchWriteArrow` instead for 5-10x better performance.
/// CSV support will be removed in v0.5.0.
#[deprecated(since = "0.2.0", note = "Use batchWriteArrow instead. CSV format is 5-10x slower than Arrow IPC. Will be removed in v0.5.0")]
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

/// Batch write data
///
/// # Deprecated
/// This function uses CSV string format which is inefficient.
/// Use `Java_io_kination_vine_VineModule_batchWriteArrow` instead for 5-10x better performance.
/// CSV support will be removed in v0.5.0.
#[deprecated(since = "0.2.0", note = "Use batchWriteArrow instead. CSV format is 5-10x slower than Arrow IPC. Will be removed in v0.5.0")]
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_batchWrite(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    data: JString,
) {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();
    let data_str: String = env.get_string(&data).expect("Fail getting data").into();
    let rows: Vec<&str> = data_str.lines().collect();
    VineBatchWriter::write(&path_str, &rows).expect("Failed to batch write");
}

// ============================================================================
// Streaming Writer JNI Functions
// ============================================================================

/// Create a new streaming writer and return its ID
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_createStreamingWriter(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
) -> jni::sys::jlong {
    let path_str: String = env.get_string(&path).expect("Fail getting path").into();

    let writer = VineStreamingWriter::new(&path_str)
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
///
/// # Deprecated
/// This function uses CSV string format which is inefficient.
/// Use `Java_io_kination_vine_VineModule_streamingAppendBatchArrow` instead for 5-10x better performance.
/// CSV support will be removed in v0.5.0.
#[deprecated(since = "0.2.0", note = "Use streamingAppendBatchArrow instead. CSV format is 5-10x slower than Arrow IPC. Will be removed in v0.5.0")]
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
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
#[allow(unused_variables)]
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
#[allow(unused_variables)]
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

// ============================================================================
// Arrow IPC JNI Functions
// ============================================================================

use arrow_bridge::{deserialize_arrow_ipc, serialize_arrow_ipc, record_batch_to_csv_rows, csv_rows_to_record_batch};
use metadata::Metadata;

/// Batch write data using Arrow IPC format
///
/// This function receives Arrow IPC bytes from JVM, deserializes to RecordBatch,
/// converts to CSV (temporary), and writes via existing Vortex writer.
///
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_batchWriteArrow(
    mut env: JNIEnv,
    class: JClass,
    path: JString,
    arrow_data: jni::sys::jbyteArray,
) {
    let path_str: String = env.get_string(&path).expect("Failed to get path").into();

    // Get Arrow IPC bytes from JVM
    let arrow_array = unsafe { jni::objects::JPrimitiveArray::from_raw(arrow_data) };
    let arrow_bytes = unsafe {
        env.get_array_elements(
            &arrow_array,
            jni::objects::ReleaseMode::NoCopyBack,
        )
        .expect("Failed to get byte array")
    };

    let byte_slice: &[u8] = unsafe {
        std::slice::from_raw_parts(arrow_bytes.as_ptr() as *const u8, arrow_bytes.len())
    };

    // Deserialize Arrow IPC to RecordBatch
    let batch = deserialize_arrow_ipc(byte_slice)
        .expect("Failed to deserialize Arrow IPC");

    // Convert to CSV rows for existing Vortex writer
    // TODO: Direct Arrow -> Vortex conversion for maximum performance
    let csv_rows = record_batch_to_csv_rows(&batch)
        .expect("Failed to convert RecordBatch to CSV");

    let rows_refs: Vec<&str> = csv_rows.iter().map(|s| s.as_str()).collect();

    // Use existing batch writer
    VineBatchWriter::write(&path_str, &rows_refs)
        .expect("Failed to batch write");
}

/// Read data and return as Arrow IPC format (preferred over CSV)
///
/// This function reads from Vortex storage, converts to Arrow RecordBatch,
/// serializes to Arrow IPC bytes, and returns to JVM.
///
/// Performance improvement: 5-10x faster than CSV string transfer
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_readDataArrow(
    mut env: JNIEnv,
    class: JClass,
    dir_path: JString,
) -> jni::sys::jbyteArray {
    let path: String = env.get_string(&dir_path).expect("Failed to get path").into();

    // Load metadata for schema
    let meta_path = format!("{}/vine_meta.json", path);
    let metadata = Metadata::load(&meta_path)
        .expect("Failed to load metadata");

    // Read data using existing reader (returns CSV strings)
    let csv_rows: Vec<String> = read_vine_data(&path);

    if csv_rows.is_empty() {
        // Return empty byte array
        return env.new_byte_array(0)
            .expect("Failed to create empty byte array")
            .into_raw();
    }

    // Convert CSV rows to RecordBatch
    let batch = csv_rows_to_record_batch(&csv_rows, &metadata)
        .expect("Failed to convert CSV to RecordBatch");

    // Serialize to Arrow IPC bytes
    let arrow_bytes = serialize_arrow_ipc(&batch)
        .expect("Failed to serialize Arrow IPC");

    // Create Java byte array and copy data
    let result = env.new_byte_array(arrow_bytes.len() as i32)
        .expect("Failed to create byte array");

    env.set_byte_array_region(&result, 0, unsafe {
        std::slice::from_raw_parts(arrow_bytes.as_ptr() as *const i8, arrow_bytes.len())
    })
    .expect("Failed to set byte array region");

    result.into_raw()
}

/// Append batch to streaming writer using Arrow IPC format
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_streamingAppendBatchArrow(
    mut env: JNIEnv,
    class: JClass,
    writer_id: jni::sys::jlong,
    arrow_data: jni::sys::jbyteArray,
) {
    // Get Arrow IPC bytes from JVM
    let arrow_array = unsafe { jni::objects::JPrimitiveArray::from_raw(arrow_data) };
    let arrow_bytes = unsafe {
        env.get_array_elements(
            &arrow_array,
            jni::objects::ReleaseMode::NoCopyBack,
        )
        .expect("Failed to get byte array")
    };

    let byte_slice: &[u8] = unsafe {
        std::slice::from_raw_parts(arrow_bytes.as_ptr() as *const u8, arrow_bytes.len())
    };

    // Deserialize Arrow IPC to RecordBatch
    let batch = deserialize_arrow_ipc(byte_slice)
        .expect("Failed to deserialize Arrow IPC");

    // Convert to CSV rows for existing writer
    let csv_rows = record_batch_to_csv_rows(&batch)
        .expect("Failed to convert RecordBatch to CSV");

    let rows_refs: Vec<&str> = csv_rows.iter().map(|s| s.as_str()).collect();

    // Use existing streaming writer
    let mut writers = STREAMING_WRITERS.lock().unwrap();
    if let Some(writer) = writers.get_mut(&writer_id) {
        writer.append_batch(&rows_refs).expect("Failed to append batch");
    } else {
        panic!("Writer ID {} not found", writer_id);
    }
}
