package io.kination.vine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

/**
 * Represents a unit of work for reading a Vine table.
 * Currently one split per table (reads all date partitions at once via JNI).
 */
public class VineSplit implements ConnectorSplit {

    private final String tablePath;

    @JsonCreator
    public VineSplit(@JsonProperty("tablePath") String tablePath) {
        this.tablePath = tablePath;
    }

    @JsonProperty
    public String getTablePath() {
        return tablePath;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return List.of();
    }

    @Override
    public Object getInfo() {
        return Map.of("tablePath", tablePath);
    }
}
