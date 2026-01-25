package io.kination.vine;

/**
 * JNI bridge to vine-core module
 * Loads native library and exposes native methods.
 *
 */
public class VineModule {
    static {
        loadNativeLibrary();
    }

    /**
     * Dynamically load native library based on OS and environment.
     * Tries multiple strategies in order:
     * 1. java.library.path system property (set in build.sbt for tests)
     * 2. Relative path from project root
     * 3. Classpath resource (for packaged JAR)
     */
    private static void loadNativeLibrary() {
        String os = System.getProperty("os.name").toLowerCase();
        String libName;
        String libExtension;

        // Determine library name based on OS
        // Main module: Linux/Unix -> libvine_core.so
        // Support MacOS, Windows for local test
        if (os.contains("mac") || os.contains("darwin")) {
            libName = "libvine_core";
            libExtension = ".dylib";
        } else if (os.contains("win")) {
            libName = "vine_core";
            libExtension = ".dll";
        } else {
            libName = "libvine_core";
            libExtension = ".so";
        }

        String fullLibName = libName + libExtension;

        try {
            System.loadLibrary("vine_core");
            System.out.println("Loaded native library from java.library.path");
            return;
        } catch (UnsatisfiedLinkError e) {
            throw new UnsatisfiedLinkError(
                "Failed to load native library -> " + fullLibName
            );
        }
    }

    /**
     * Create a new streaming writer and return its ID.
     * The writer must be closed with streamingClose() when done.
     *
     * @param path Directory path to Vine table
     * @return Writer ID (for subsequent operations)
     */
    public static native long createStreamingWriter(String path);

    /**
     * Flush streaming writer (closes current file, opens new on next write)
     *
     * @param writerId Writer ID from createStreamingWriter()
     */
    public static native void streamingFlush(long writerId);

    /**
     * Close and remove streaming writer.
     * All pending data will be flushed.
     *
     * @param writerId Writer ID from createStreamingWriter()
     */
    public static native void streamingClose(long writerId);

    /**
     * Read data from Vine table using Arrow IPC format.
     *
     * @param path Directory path to Vine table
     * @return Arrow IPC stream bytes containing RecordBatch data
     */
    public static native byte[] readDataArrow(String path);

    /**
     * Batch write to Vine table using Arrow IPC format.
     *
     * @param path Directory path to Vine table
     * @param arrowData Arrow IPC stream bytes containing RecordBatch data
     */
    public static native void batchWriteArrow(String path, byte[] arrowData);

    /**
     * Append batch of rows to streaming writer, using Arrow IPC format.
     *
     * @param writerId Writer ID from createStreamingWriter()
     * @param arrowData Arrow IPC stream bytes containing RecordBatch data
     */
    public static native void streamingAppendBatchArrow(long writerId, byte[] arrowData);
}
