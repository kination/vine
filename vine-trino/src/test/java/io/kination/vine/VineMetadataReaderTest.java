package io.kination.vine;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VineMetadataReaderTest {

    @Test
    void testReadMetadata() {
        String testTablePath = getTestResourcePath("test_table");

        VineMetadata metadata = VineMetadataReader.read(testTablePath);

        assertEquals("test_table", metadata.getTableName());
        assertNotNull(metadata.getFields());
        assertEquals(4, metadata.getFields().size());
    }

    @Test
    void testFieldParsing() {
        String testTablePath = getTestResourcePath("test_table");

        VineMetadata metadata = VineMetadataReader.read(testTablePath);
        List<VineMetadata.Field> fields = metadata.getFields();

        // Field 1: id
        assertEquals(1, fields.get(0).getId());
        assertEquals("id", fields.get(0).getName());
        assertEquals("integer", fields.get(0).getDataType());
        assertTrue(fields.get(0).isRequired());

        // Field 2: name
        assertEquals(2, fields.get(1).getId());
        assertEquals("name", fields.get(1).getName());
        assertEquals("string", fields.get(1).getDataType());
        assertFalse(fields.get(1).isRequired());

        // Field 3: score
        assertEquals(3, fields.get(2).getId());
        assertEquals("score", fields.get(2).getName());
        assertEquals("double", fields.get(2).getDataType());
        assertFalse(fields.get(2).isRequired());

        // Field 4: active
        assertEquals(4, fields.get(3).getId());
        assertEquals("active", fields.get(3).getName());
        assertEquals("boolean", fields.get(3).getDataType());
        assertFalse(fields.get(3).isRequired());
    }

    @Test
    void testHasMetadata() {
        String testTablePath = getTestResourcePath("test_table");
        assertTrue(VineMetadataReader.hasMetadata(testTablePath));
    }

    @Test
    void testHasMetadataMissing() {
        assertFalse(VineMetadataReader.hasMetadata("/nonexistent/path"));
    }

    @Test
    void testReadNonExistentPath() {
        assertThrows(IllegalArgumentException.class,
                () -> VineMetadataReader.read("/nonexistent/path"));
    }

    private String getTestResourcePath(String resourceName) {
        URL url = getClass().getClassLoader().getResource(resourceName);
        if (url == null) {
            throw new RuntimeException("Test resource not found: " + resourceName);
        }
        return url.getPath();
    }
}
