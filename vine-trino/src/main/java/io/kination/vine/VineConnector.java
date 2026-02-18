package io.kination.vine;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

/**
 * Read-only Vine connector for Trino.
 * This connector allows Trino to query Vine tables by reading Arrow IPC data produced by vine-core.
 */
public class VineConnector implements Connector {

    private final VineConnectorMetadata metadata;
    private final VineSplitManager splitManager;
    private final VineRecordSetProvider recordSetProvider;

    public VineConnector(String dataDir) {
        this.metadata = new VineConnectorMetadata(dataDir);
        this.splitManager = new VineSplitManager();
        this.recordSetProvider = new VineRecordSetProvider();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
                                                        boolean readOnly, boolean autoCommit) {
        return VineTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }
}
