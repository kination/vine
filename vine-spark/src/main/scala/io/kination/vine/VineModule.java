package io.kination.vine;

public class VineModule {
    static {
        // TODO: make path depend to root
        System.load(
            "/Users/kination/workspace/public/vine/vine-core/target/release/libvine_core.dylib"
        );
    }

    public static native String readDataFromVine(String path);
    public static native void writeDataToVine(String path, String data);

    public static String readData(String path) {
        return readDataFromVine(path);
    }

    public static void writeData(String path, String data) {
        writeDataToVine(path, data);
    }
}
