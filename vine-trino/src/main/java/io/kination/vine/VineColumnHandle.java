package io.kination.vine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

/**
 * Handle representing column in Vine table.
 */
public class VineColumnHandle implements ColumnHandle {

    private final String name;
    private final Type type;
    private final int ordinalPosition;

    @JsonCreator
    public VineColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinalPosition") int ordinalPosition) {
        this.name = name;
        this.type = type;
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VineColumnHandle that = (VineColumnHandle) o;
        return ordinalPosition == that.ordinalPosition
                && Objects.equals(name, that.name)
                && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, ordinalPosition);
    }

    @Override
    public String toString() {
        return name + ":" + type;
    }
}
