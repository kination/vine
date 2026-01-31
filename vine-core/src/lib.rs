pub mod metadata;
pub mod writer_config;
pub mod writer_cache;
pub mod streaming_writer;
pub mod streaming_writer_v2;
pub mod vine_batch_writer;
pub mod vine_streaming_writer;
pub mod reader_cache;
pub mod storage_reader;
pub mod global_cache;
pub mod vortex_exp;
pub mod arrow_bridge;

use std::ffi::CString;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jobject;

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
//
// Note: CSV-based batch write functions have been removed in favor of Arrow IPC.
// Use batchWriteArrow for better performance (5-10x faster than CSV format).
// ============================================================================

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

use arrow_bridge::{deserialize_arrow_ipc, serialize_arrow_ipc, arrow_to_storage_format, storage_format_to_arrow};
use metadata::Metadata;

/// Batch write data using Arrow IPC format
///
/// This function receives Arrow IPC bytes from JVM, deserializes to RecordBatch,
/// converts to storage format (currently CSV), and writes via Vortex writer.
///
/// TODO: 
/// Update arrow_to_storage_format() to make direct Arrow → Vortex conversion
/// Migration process (when Vortex API is ready)
///     1. Update arrow_bridge::arrow_to_storage_format() to use direct Arrow → Vortex
///     2. Update VineBatchWriter to accept Vortex arrays instead of CSV
///     3. No changes needed in this function - it will automatically benefit
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

    // Convert Arrow to storage format (currently CSV, future: direct Vortex)
    // TODO: This will automatically use direct conversion once arrow_to_storage_format() is updated
    let storage_data = arrow_to_storage_format(&batch)
        .expect("Failed to convert Arrow to storage format");

    let rows_refs: Vec<&str> = storage_data.iter().map(|s| s.as_str()).collect();

    // Write to storage
    // TODO: Update VineBatchWriter to accept Vortex arrays when direct conversion is ready
    VineBatchWriter::write(&path_str, &rows_refs)
        .expect("Failed to batch write");
}

/// Read data and return as Arrow IPC format
///
/// This function reads from Vortex storage, converts to Arrow RecordBatch,
/// serializes to Arrow IPC bytes, and returns to JVM.
///
/// TODO: 
/// Update storage_format_to_arrow() to make direct Vortex → Arrow conversion
/// Migration path (when Vortex API is ready)
///     1. Update storage reader to return Vortex arrays instead of CSV
///     2. Update arrow_bridge::storage_format_to_arrow() to use direct Vortex → Arrow
///     3. No changes needed in this function - it will automatically benefit
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

    // Read from storage (currently returns CSV, future: will return Vortex arrays)
    // TODO: Update read_vine_data() to return Vortex arrays when direct conversion is ready
    let storage_data: Vec<String> = read_vine_data(&path);

    if storage_data.is_empty() {
        // Return empty byte array
        return env.new_byte_array(0)
            .expect("Failed to create empty byte array")
            .into_raw();
    }

    // Convert storage format to Arrow (currently from CSV, future: direct from Vortex)
    let batch = storage_format_to_arrow(&storage_data, &metadata)
        .expect("Failed to convert storage format to Arrow");

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
///
/// TODO: 
/// Update arrow_to_storage_format() to make direct Arrow → Vortex conversion
/// Migration path (when Vortex API is ready)
///     1. Update arrow_bridge::arrow_to_storage_format() to use direct Arrow → Vortex
///     2. Update VineStreamingWriter to accept Vortex arrays instead of CSV
/// 3. No changes needed in this function - it will automatically benefit
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

    // Convert Arrow to storage format (currently CSV, future: direct Vortex)
    let storage_data = arrow_to_storage_format(&batch)
        .expect("Failed to convert Arrow to storage format");

    let rows_refs: Vec<&str> = storage_data.iter().map(|s| s.as_str()).collect();

    // Use existing streaming writer
    // TODO: Update VineStreamingWriter to accept Vortex arrays when direct conversion is ready
    let mut writers = STREAMING_WRITERS.lock().unwrap();
    if let Some(writer) = writers.get_mut(&writer_id) {
        writer.append_batch(&rows_refs).expect("Failed to append batch");
    } else {
        panic!("Writer ID {} not found", writer_id);
    }
}
