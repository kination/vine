package io.kination.vine;

public class VineModule {
    static {
        loadNativeLibrary();
    }

    private static void loadNativeLibrary() {
        String os = System.getProperty("os.name").toLowerCase();
        String libName;
        String libExtension;

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
            System.out.println("[VineTrino] Loaded native library from java.library.path");
            return;
        } catch (UnsatisfiedLinkError e) {
            throw new UnsatisfiedLinkError(
                "Failed to load native library -> " + fullLibName +
                ". Ensure vine-core is built: cd vine-core && cargo build --release"
            );
        }
    }

    /**
     * Read data from Vine table using Arrow IPC format.
     *
     * @param path Directory path to Vine table (must contain vine_meta.json)
     * @return Arrow IPC stream bytes containing RecordBatch data
     */
    public static native byte[] readDataArrow(String path);
}
