package io.kination.vine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

/**
 * Handle representing Vine table in Trino.
 * Contains the filesystem path to table directory.
 */
public class VineTableHandle implements ConnectorTableHandle {

    private final String schemaName;
    private final String tableName;
    private final String tablePath;

    @JsonCreator
    public VineTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tablePath") String tablePath) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tablePath = tablePath;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getTablePath() {
        return tablePath;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VineTableHandle that = (VineTableHandle) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(tablePath, that.tablePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName, tablePath);
    }

    @Override
    public String toString() {
        return schemaName + "." + tableName;
    }
}
