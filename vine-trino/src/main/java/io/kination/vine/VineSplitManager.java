package io.kination.vine;

import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;

/**
 * Split manager for Vine tables.
 * Currently generates 'single split' per table.
 * vine-core's readDataArrow reads all date partitions at once.
 */
public class VineSplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        VineTableHandle vineTable = (VineTableHandle) table;
        ConnectorSplit split = new VineSplit(vineTable.getTablePath());
        return new FixedSplitSource(List.of(split));
    }
}
