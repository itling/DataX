package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Schema cache for TDengine 3.X
 */
public final class SchemaCache {
    private static final Logger log = LoggerFactory.getLogger(TDengineWriter.Job.class);

    private static volatile SchemaCache instance;

    private final Configuration config;
    private final Connection conn;
    private final String dbname;
    private SchemaManager schemaManager;

    // table name -> TableMeta
    private final Map<String, TableMeta> tableMetas = new HashMap<>();
    // table name ->List<ColumnMeta>
    private final Map<String, List<ColumnMeta>> columnMetas = new HashMap<>();

    private SchemaCache(Configuration config) {
        this.config = config;

        // connect
        final String user = config.getString(Key.USERNAME, Constants.DEFAULT_USERNAME);
        final String pass = config.getString(Key.PASSWORD, Constants.DEFAULT_PASSWORD);

        Configuration connConfig = Configuration.from(config.getList(Key.CONNECTION).get(0).toString());
        final String url = connConfig.getString(Key.JDBC_URL);
        try {
            this.conn = DriverManager.getConnection(url, user, pass);
        } catch (SQLException e) {
            throw DataXException.asDataXException(
                    "failed to connect to url: " + url + ", cause: {" + e.getMessage() + "}");
        }

        this.dbname = TDengineWriter.parseDatabaseFromJdbcUrl(url);
        
        // Create appropriate SchemaManager based on TDengine version
        try (Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select " + Constants.SERVER_VERSION);
            resultSet.next();
            String serverVersion = resultSet.getString(Constants.SERVER_VERSION);
            if (serverVersion.startsWith(Constants.SERVER_VERSION_2)) {
                this.schemaManager = new SchemaManager(conn);
            } else {
                this.schemaManager = new Schema3_0Manager(conn, dbname);
            }
        } catch (SQLException e) {
            // Fallback to Schema3_0Manager if version check fails
            this.schemaManager = new Schema3_0Manager(conn, dbname);
            log.warn("Failed to check TDengine version, using Schema3_0Manager as fallback: " + e.getMessage());
        }

        // init table meta cache and load
        final List<String> tables = connConfig.getList(Key.TABLE, String.class);
        Map<String, TableMeta> loadedTableMetas = schemaManager.loadTableMeta(tables);
        this.tableMetas.putAll(loadedTableMetas);
    }

    public static SchemaCache getInstance(Configuration originConfig) {
        if (instance == null) {
            synchronized (SchemaCache.class) {
                if (instance == null) {
                    instance = new SchemaCache(originConfig);
                }
            }
        }
        return instance;
    }

    public TableMeta getTableMeta(String table_name) {
        if (!tableMetas.containsKey(table_name)) {
            throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION,
                    "table metadata of " + table_name + " is empty!");
        }

        return tableMetas.get(table_name);
    }

    public List<ColumnMeta> getColumnMetaList(String tbname, TableType tableType) {
        if (!columnMetas.containsKey(tbname) || columnMetas.get(tbname).isEmpty()) {
            synchronized (this) {
                if (!columnMetas.containsKey(tbname) || columnMetas.get(tbname).isEmpty()) {
                    List<ColumnMeta> colMetaList = getColumnMetaListFromDb(tbname, tableType);
                    if (colMetaList.isEmpty()) {
                        throw DataXException.asDataXException("column metadata of table: " + tbname + " is empty!");
                    }
                    columnMetas.put(tbname, colMetaList);
                }
            }
        }

        return columnMetas.get(tbname);
    }

    private List<ColumnMeta> getColumnMetaListFromDb(String tableName, TableType tableType) {
        List<ColumnMeta> columnMetaList = new ArrayList<>();
        // Use a set to track added column names to avoid duplicates
        Set<String> addedColumns = new HashSet<>();

        List<String> column_name = config.getList(Key.COLUMN, String.class)
                                         .stream()
                                         .map(String::toLowerCase)
                                         .collect(Collectors.toList());

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            // Get columns for the table
            ResultSet rs = metaData.getColumns(dbname, null, tableName, "%");
            int primaryKeyIndex = 0;
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                // Skip if column already added
                if (addedColumns.contains(columnName.toLowerCase())) {
                    continue;
                }
                
                String columnType = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                
                // Check if this column is a tag
                boolean isTag = isColumnTag(tableName, columnName);
                
                // Check if this is the primary key (first column is usually the primary key in TDengine)
                boolean isPrimaryKey = primaryKeyIndex == 0;
                primaryKeyIndex++;
                
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.field = columnName;
                columnMeta.type = columnType;
                columnMeta.length = columnSize;
                columnMeta.note = isTag ? Constants.COLUMN_META_NOTE_TAG : "";
                columnMeta.isTag = isTag;
                columnMeta.isPrimaryKey = isPrimaryKey;
                
                if (column_name.contains(columnMeta.field.toLowerCase())) {
                    columnMetaList.add(columnMeta);
                    addedColumns.add(columnName.toLowerCase());
                }
            }
            rs.close();
        } catch (SQLException e) {
            // Fallback to describe statement if DatabaseMetaData fails
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("describe " + tableName);
                for (int i = 0; rs.next(); i++) {
                    ColumnMeta columnMeta = buildColumnMeta(rs, i == 0);
                    // Skip if column already added
                    if (addedColumns.contains(columnMeta.field.toLowerCase())) {
                        continue;
                    }
                    
                    if (column_name.contains(columnMeta.field.toLowerCase())) {
                        columnMetaList.add(columnMeta);
                        addedColumns.add(columnMeta.field.toLowerCase());
                    }
                }
                rs.close();
            } catch (SQLException ex) {
                throw DataXException.asDataXException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, ex.getMessage());
            }
        }

        // 如果是子表，才需要获取 tag 值
        if (tableType == TableType.SUB_TABLE) {
            for (ColumnMeta colMeta : columnMetaList) {
                if (!colMeta.isTag)
                    continue;
                Object tagValue = getTagValue(tableName, colMeta.field);
                colMeta.value = tagValue;
            }
        }

        return columnMetaList;
    }

    private boolean isColumnTag(String tableName, String columnName) {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("describe " + tableName);
            while (rs.next()) {
                String field = rs.getString(Constants.COLUMN_META_FIELD);
                String note = rs.getString(Constants.COLUMN_META_NOTE);
                if (field.equals(columnName) && Constants.COLUMN_META_NOTE_TAG.equals(note)) {
                    return true;
                }
            }
            rs.close();
        } catch (SQLException e) {
            log.error("failed to check if column is tag, cause: {" + e.getMessage() + "}");
        }
        return false;
    }

    private Object getTagValue(String tableName, String tagName) {
        String sql = "select " + tagName + " from " + tableName + " limit 1";
        Object tagValue = null;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                tagValue = rs.getObject(tagName);
            }
            rs.close();
        } catch (SQLException e) {
            log.error("failed to get tag value, use NULL, cause: {" + e.getMessage() + "}");
        }
        return tagValue;
    }

    private ColumnMeta buildColumnMeta(ResultSet rs, boolean isPrimaryKey) throws SQLException {
        ColumnMeta columnMeta = new ColumnMeta();
        columnMeta.field = rs.getString(Constants.COLUMN_META_FIELD);
        columnMeta.type = rs.getString(Constants.COLUMN_META_TYPE);
        columnMeta.length = rs.getInt(Constants.COLUMN_META_LENGTH);
        columnMeta.note = rs.getString(Constants.COLUMN_META_NOTE);
        columnMeta.isTag = Constants.COLUMN_META_NOTE_TAG.equals(columnMeta.note);
        columnMeta.isPrimaryKey = isPrimaryKey;
        return columnMeta;
    }
}
