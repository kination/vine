package io.kination.vine;

/**
 * JNI bridge to Rust vine-core library.
 *
 * This module provides low-level access to native Vine functions.
 * For high-level Scala API, use VineWriter and VineReader classes.
 */
public class VineModule {
    static {
        // TODO: make path depend to root
        System.load(
            "/Users/kination/workspace/public/vine/vine-core/target/release/libvine_core.dylib"
        );
    }

    // ============================================================================
    // Reader JNI Functions
    // ============================================================================

    /**
     * Read data from Vine table and return as CSV string.
     * @param path Directory path to Vine table
     * @return CSV-formatted data (one row per line)
     */
    public static native String readDataFromVine(String path);

    // ============================================================================
    // Batch Writer JNI Functions
    // ============================================================================

    /**
     * Legacy batch write function (backward compatible).
     * Uses balanced configuration internally.
     * @param path Directory path to Vine table
     * @param data CSV-formatted data (one row per line)
     */
    public static native void writeDataToVine(String path, String data);

    /**
     * Batch write with balanced configuration.
     * Good for production streaming workloads.
     * Config: SNAPPY compression, 100K row groups
     * @param path Directory path to Vine table
     * @param data CSV-formatted data (one row per line)
     */
    public static native void batchWriteBalanced(String path, String data);

    /**
     * Batch write with high throughput configuration.
     * Optimized for maximum write speed.
     * Config: No compression, 50K row groups
     * @param path Directory path to Vine table
     * @param data CSV-formatted data (one row per line)
     */
    public static native void batchWriteHighThroughput(String path, String data);

    /**
     * Batch write with high compression configuration.
     * Optimized for storage efficiency.
     * Config: ZSTD compression, 1M row groups
     * @param path Directory path to Vine table
     * @param data CSV-formatted data (one row per line)
     */
    public static native void batchWriteHighCompression(String path, String data);

    // ============================================================================
    // Streaming Writer JNI Functions
    // ============================================================================

    /**
     * Create a new streaming writer and return its ID.
     * The writer must be closed with streamingClose() when done.
     * @param path Directory path to Vine table
     * @param configType 0=balanced, 1=high_throughput, 2=high_compression
     * @return Writer ID (use for subsequent operations)
     */
    public static native long createStreamingWriter(String path, int configType);

    /**
     * Append a batch of rows to existing streaming writer.
     * @param writerId Writer ID from createStreamingWriter()
     * @param data CSV-formatted data (one row per line)
     */
    public static native void streamingAppendBatch(long writerId, String data);

    /**
     * Flush streaming writer (closes current file, opens new on next write).
     * @param writerId Writer ID from createStreamingWriter()
     */
    public static native void streamingFlush(long writerId);

    /**
     * Close and remove streaming writer.
     * All pending data will be flushed.
     * @param writerId Writer ID from createStreamingWriter()
     */
    public static native void streamingClose(long writerId);

    // ============================================================================
    // Legacy API (Backward Compatibility)
    // ============================================================================

    /**
     * @deprecated Use batchWriteBalanced() or VineWriter API instead
     */
    @Deprecated
    public static String readData(String path) {
        return readDataFromVine(path);
    }

    /**
     * @deprecated Use batchWriteBalanced() or VineWriter API instead
     */
    @Deprecated
    public static void writeData(String path, String data) {
        writeDataToVine(path, data);
    }
}
