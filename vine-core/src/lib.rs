pub mod metadata;
pub mod writer_config;
pub mod writer_cache;
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

/// Read data from Vine storage and return as string (legacy compat)
///
/// Reads Vortex arrays from storage, converts to Arrow RecordBatch
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

    let arrays = read_vine_data(&path);
    let mut result: String = String::new();

    for array in arrays {
        // use compat mode (true) for legacy string formatting
        // TODO: Add option to disable compat mode and use arrow_cast display
        match vortex_to_arrow(&array, true) {
            Ok(batch) => {
                // Use arrow_cast display for proper formatting
                let options = arrow_cast::display::FormatOptions::default();
                let formatters: Vec<_> = batch
                    .columns()
                    .iter()
                    .map(|c| arrow_cast::display::ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
                    .collect();

                for row_idx in 0..batch.num_rows() {
                    let mut row_values: Vec<String> = Vec::with_capacity(batch.num_columns());
                    for formatter in &formatters {
                        row_values.push(formatter.value(row_idx).to_string());
                    }
                    result.push_str(&row_values.join(","));
                    result.push('\n');
                }
            }
            Err(e) => {
                eprintln!("Error converting Vortex to Arrow: {}", e);
            }
        }
    }
    let output = CString::new(result).expect("Cannot generate CString from result");

    env.new_string(output.to_str().unwrap())
        .expect("Cannot create java string")
        .into_raw()
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

use arrow_bridge::{deserialize_arrow_ipc, serialize_arrow_ipc, arrow_to_vortex, vortex_to_arrow};

/// Batch write data using Arrow IPC format
///
/// Receives Arrow IPC bytes from JVM, converts directly to Vortex array,
/// and writes to storage.
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

    // Arrow → Vortex conversion
    let vortex_array = arrow_to_vortex(&batch)
        .expect("Failed to convert Arrow to Vortex");

    // Write Vortex array directly to storage
    VineBatchWriter::write(&path_str, &vortex_array)
        .expect("Failed to batch write");
}

/// Read data and return as Arrow IPC format
///
/// Reads Vortex arrays from storage, converts directly to Arrow RecordBatch,
/// serializes to Arrow IPC bytes, and returns to JVM.
///
/// # Arguments
/// * `compat_mode` - 0=None, 1=Compat (Java14/Spark3.5, Utf8View -> Utf8)
#[no_mangle]
#[allow(non_snake_case)]
#[allow(unused_variables)]
pub extern "C" fn Java_io_kination_vine_VineModule_readDataArrow(
    mut env: JNIEnv,
    class: JClass,
    dir_path: JString,
    compat_mode: jni::sys::jint, 
) -> jni::sys::jbyteArray {
    let path: String = env.get_string(&dir_path).expect("Failed to get path").into();

    // Read Vortex arrays directly from storage
    let arrays = read_vine_data(&path);

    if arrays.is_empty() {
        return env.new_byte_array(0)
            .expect("Failed to create empty byte array")
            .into_raw();
    }

    // Convert first Vortex array to Arrow RecordBatch
    // TODO: Support multiple arrays by concatenating
    
    // Determine compatibility mode from JNI argument
    let use_compat = compat_mode == 1;

    let batch = vortex_to_arrow(&arrays[0], use_compat)
        .expect("Failed to convert Vortex to Arrow");

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
/// Receives Arrow IPC bytes, converts directly to Vortex array,
/// and appends to the streaming writer.
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

    // Arrow → Vortex conversion
    let vortex_array = arrow_to_vortex(&batch)
        .expect("Failed to convert Arrow to Vortex");

    // Append Vortex array directly to streaming writer
    let mut writers = STREAMING_WRITERS.lock().unwrap();
    if let Some(writer) = writers.get_mut(&writer_id) {
        writer.append_batch(&vortex_array).expect("Failed to append batch");
    } else {
        panic!("Writer ID {} not found", writer_id);
    }
}
