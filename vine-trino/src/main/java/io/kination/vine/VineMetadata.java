package io.kination.vine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * POJO representing vine_meta.json schema definition.
 * This class is used to deserialize vine_meta.json.
 */
public class VineMetadata {

    private final String tableName;
    private final List<Field> fields;

    @JsonCreator
    public VineMetadata(
            @JsonProperty("table_name") String tableName,
            @JsonProperty("fields") List<Field> fields) {
        this.tableName = tableName;
        this.fields = fields;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Field> getFields() {
        return fields;
    }

    public static class Field {
        private final int id;
        private final String name;
        private final String dataType;
        private final boolean isRequired;

        @JsonCreator
        public Field(
                @JsonProperty("id") int id,
                @JsonProperty("name") String name,
                @JsonProperty("data_type") String dataType,
                @JsonProperty("is_required") boolean isRequired) {
            this.id = id;
            this.name = name;
            this.dataType = dataType;
            this.isRequired = isRequired;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getDataType() {
            return dataType;
        }

        public boolean isRequired() {
            return isRequired;
        }
    }
}
