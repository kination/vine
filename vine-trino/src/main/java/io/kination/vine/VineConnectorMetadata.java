package io.kination.vine;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Provides schema discovery and metadata
 * Lists tables by scanning data directory for subdirectories containing vine_meta.json.
 *
 * Directory layout:
 * <pre>
 * vine.data-dir/
 *   table_one/vine_meta.json
 *   table_two/vine_meta.json
 * </pre>
 */
public class VineConnectorMetadata implements ConnectorMetadata {

    private static final String DEFAULT_SCHEMA = "default";

    private final String dataDir;

    public VineConnectorMetadata(String dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return List.of(DEFAULT_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        if (!DEFAULT_SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }
        String tablePath = dataDir + File.separator + tableName.getTableName();
        if (!VineMetadataReader.hasMetadata(tablePath)) {
            return null;
        }

        return new VineTableHandle(tableName.getSchemaName(), tableName.getTableName(), tablePath);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        VineTableHandle vineTable = (VineTableHandle) table;
        List<ColumnMetadata> columns = getColumns(vineTable);

        return new ConnectorTableMetadata(vineTable.toSchemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        VineTableHandle vineTable = (VineTableHandle) tableHandle;
        VineMetadata meta = VineMetadataReader.read(vineTable.getTablePath());

        List<VineMetadata.Field> fields = meta.getFields();

        return IntStream.range(0, fields.size())
                .boxed()
                .collect(Collectors.toMap(
                        i -> fields.get(i).getName(),
                        i -> (ColumnHandle) new VineColumnHandle(
                                fields.get(i).getName(),
                                VineTypeMapping.toTrinoType(fields.get(i).getDataType()),
                                i),
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                             ConnectorTableHandle tableHandle,
                                             ColumnHandle columnHandle) {
        VineColumnHandle col = (VineColumnHandle) columnHandle;
        return new ColumnMetadata(col.getName(), col.getType());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        if (schemaName.isPresent() && !DEFAULT_SCHEMA.equals(schemaName.get())) {
            return List.of();
        }

        List<SchemaTableName> tables = new ArrayList<>();
        File dir = new File(dataDir);
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    if (child.isDirectory() && VineMetadataReader.hasMetadata(child.getAbsolutePath())) {
                        tables.add(new SchemaTableName(DEFAULT_SCHEMA, child.getName()));
                    }
                }
            }
        }
        return tables;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix) {
        Map<SchemaTableName, List<ColumnMetadata>> result = new LinkedHashMap<>();
        for (SchemaTableName tableName : listTables(session, Optional.of(prefix.getSchema().orElse(DEFAULT_SCHEMA)))) {
            if (prefix.getTable().isPresent() && !prefix.getTable().get().equals(tableName.getTableName())) {
                continue;
            }
            ConnectorTableHandle handle = getTableHandle(session, tableName);
            if (handle != null) {
                result.put(tableName, getColumns((VineTableHandle) handle));
            }
        }
        return result;
    }

    private List<ColumnMetadata> getColumns(VineTableHandle tableHandle) {
        VineMetadata meta = VineMetadataReader.read(tableHandle.getTablePath());
        return meta.getFields().stream()
                .map(f -> new ColumnMetadata(f.getName(), VineTypeMapping.toTrinoType(f.getDataType())))
                .collect(Collectors.toList());
    }
}
