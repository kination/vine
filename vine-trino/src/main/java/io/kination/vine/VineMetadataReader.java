package io.kination.vine;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Reads and parses vine_meta.json from a Vine table directory.
 */
public class VineMetadataReader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String META_FILE_NAME = "vine_meta.json";

    public static VineMetadata read(String tablePath) {
        File metaFile = new File(tablePath, META_FILE_NAME);
        if (!metaFile.exists()) {
            throw new IllegalArgumentException("vine_meta.json not found at: " + tablePath);
        }
        try {
            return MAPPER.readValue(metaFile, VineMetadata.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read vine_meta.json at: " + tablePath, e);
        }
    }

    public static boolean hasMetadata(String tablePath) {
        return new File(tablePath, META_FILE_NAME).exists();
    }
}
