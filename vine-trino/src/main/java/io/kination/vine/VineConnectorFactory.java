package io.kination.vine;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

/**
 * VineConnectorFactory: To create 'VineConnector' instances from catalog configuration.
 *
 * Catalog config (etc/catalog/vine.properties):
 * <pre>
 * connector.name=vine
 * vine.data-dir=/path/to/vine/tables
 * </pre>
 */
public class VineConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "vine";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        String dataDir = config.get("vine.data-dir");
        if (dataDir == null || dataDir.isEmpty()) {
            throw new IllegalArgumentException("vine.data-dir must be set in catalog configuration");
        }
        return new VineConnector(dataDir);
    }
}
