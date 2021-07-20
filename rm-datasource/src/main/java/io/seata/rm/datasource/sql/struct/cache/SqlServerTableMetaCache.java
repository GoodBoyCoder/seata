package io.seata.rm.datasource.sql.struct.cache;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.StringUtils;
import io.seata.rm.datasource.ColumnUtils;
import io.seata.rm.datasource.sql.struct.ColumnMeta;
import io.seata.rm.datasource.sql.struct.IndexMeta;
import io.seata.rm.datasource.sql.struct.IndexType;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.sqlparser.util.JdbcConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The type SqlServer Table meta cache.
 *
 * @author GoodBoyCoder
 */
@LoadLevel(name = JdbcConstants.SQLSERVER)
public class SqlServerTableMetaCache extends AbstractTableMetaCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerTableMetaCache.class);

    @Override
    protected String getCacheKey(Connection connection, String tableName, String resourceId) {
        StringBuilder cacheKey = new StringBuilder(resourceId);
        cacheKey.append(".");

        //separate it to schemaName and tableName
        String[] tableNameWithSchema = tableName.split("\\.");
        String defaultTableName = tableNameWithSchema[tableNameWithSchema.length - 1];

        DatabaseMetaData databaseMetaData;
        try {
            databaseMetaData = connection.getMetaData();
        } catch (SQLException e) {
            LOGGER.error("Could not get connection, use default cache key {}", e.getMessage(), e);
            return cacheKey.append(defaultTableName).toString();
        }

        try {
            //prevent duplicated cache key
            if (databaseMetaData.supportsMixedCaseIdentifiers()) {
                cacheKey.append(defaultTableName);
            } else {
                cacheKey.append(defaultTableName.toUpperCase());
            }
        } catch (SQLException e) {
            LOGGER.error("Could not get supportsMixedCaseIdentifiers in connection metadata, use default cache key {}", e.getMessage(), e);
            return cacheKey.append(defaultTableName).toString();
        }

        return cacheKey.toString();
    }

    @Override
    protected TableMeta fetchSchema(Connection connection, String tableName) throws SQLException {
        try {
            return resultSetMetaToSchema(connection, tableName);
        } catch (SQLException sqlEx) {
            throw sqlEx;
        } catch (Exception e) {
            throw new SQLException(String.format("Failed to fetch schema of %s", tableName), e);
        }
    }

    private TableMeta resultSetMetaToSchema(Connection connection, String tableName) throws SQLException {
        TableMeta tm = new TableMeta();
        tm.setTableName(tableName);

        tableName = ColumnUtils.delEscape(tableName, JdbcConstants.SQLSERVER);
        String[] schemaTable = tableName.split("\\.");
        //get catalog and schema from tableName
        String catalogName = "";
        String schemaName = "";
        if (schemaTable.length > 2) {
            catalogName = schemaTable[schemaTable.length - 3];
        } else if (schemaTable.length > 1) {
            schemaName = schemaTable[schemaTable.length - 2];
        }

        //If the tableName does not contain the required information，get from connection
        if (StringUtils.isBlank(catalogName)) {
            catalogName = connection.getCatalog();
        }
        if (StringUtils.isBlank(schemaName)) {
            schemaName = connection.getSchema();
        }
        //get pure tableName
        String pureTableName = schemaTable[schemaTable.length - 1];

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rsColumns = metaData.getColumns(catalogName, schemaName, pureTableName, "%");
             ResultSet rsIndex = metaData.getIndexInfo(catalogName, schemaName, pureTableName, false, true);
             ResultSet rsPrimary = metaData.getPrimaryKeys(catalogName, schemaName, pureTableName)) {
            //get column metaData
            while (rsColumns.next()) {
                ColumnMeta col = new ColumnMeta();
                col.setTableCat(rsColumns.getString("TABLE_CAT"));
                col.setTableSchemaName(rsColumns.getString("TABLE_SCHEM"));
                col.setTableName(rsColumns.getString("TABLE_NAME"));
                col.setColumnName(rsColumns.getString("COLUMN_NAME"));
                col.setDataType(rsColumns.getInt("DATA_TYPE"));
                col.setDataTypeName(rsColumns.getString("TYPE_NAME"));
                col.setColumnSize(rsColumns.getInt("COLUMN_SIZE"));
                col.setDecimalDigits(rsColumns.getInt("DECIMAL_DIGITS"));
                col.setNumPrecRadix(rsColumns.getInt("NUM_PREC_RADIX"));
                col.setNullAble(rsColumns.getInt("NULLABLE"));
                //always return NULL for REMARKS label
                col.setRemarks(rsColumns.getString("REMARKS"));
                col.setColumnDef(rsColumns.getString("COLUMN_DEF"));
                col.setSqlDataType(rsColumns.getInt("SQL_DATA_TYPE"));
                col.setSqlDatetimeSub(rsColumns.getInt("SQL_DATETIME_SUB"));
                col.setCharOctetLength(rsColumns.getInt("CHAR_OCTET_LENGTH"));
                col.setOrdinalPosition(rsColumns.getInt("ORDINAL_POSITION"));
                col.setIsNullAble(rsColumns.getString("IS_NULLABLE"));
                col.setIsAutoincrement(rsColumns.getString("IS_AUTOINCREMENT"));

                if (tm.getAllColumns().containsKey(col.getColumnName())) {
                    throw new NotSupportYetException("Not support the table has the same column name with different case yet");
                }
                tm.getAllColumns().put(col.getColumnName(), col);
            }

            //get index metaData
            while (rsIndex.next()) {
                String indexName = rsIndex.getString("INDEX_NAME");
                if (StringUtils.isNullOrEmpty(indexName)) {
                    continue;
                }
                String colName = rsIndex.getString("COLUMN_NAME");
                ColumnMeta col = tm.getAllColumns().get(colName);
                if (tm.getAllIndexes().containsKey(indexName)) {
                    IndexMeta index = tm.getAllIndexes().get(indexName);
                    index.getValues().add(col);
                } else {
                    IndexMeta index = new IndexMeta();
                    index.setIndexName(indexName);
                    index.setNonUnique(rsIndex.getBoolean("NON_UNIQUE"));
                    index.setIndexQualifier(rsIndex.getString("INDEX_QUALIFIER"));
                    index.setIndexName(rsIndex.getString("INDEX_NAME"));
                    index.setType(rsIndex.getShort("TYPE"));
                    //start from '1'
                    index.setOrdinalPosition(rsIndex.getShort("ORDINAL_POSITION"));
                    //SqlServer always return 'A', means Ascending
                    index.setAscOrDesc(rsIndex.getString("ASC_OR_DESC"));
                    index.setCardinality(rsIndex.getInt("CARDINALITY"));
                    index.getValues().add(col);
                    if (!index.isNonUnique()) {
                        index.setIndextype(IndexType.UNIQUE);
                    } else {
                        index.setIndextype(IndexType.NORMAL);
                    }
                    tm.getAllIndexes().put(indexName, index);
                }
            }

            while (rsPrimary.next()) {
                String pkIndexName = rsPrimary.getString("PK_NAME");
                if (tm.getAllIndexes().containsKey(pkIndexName)) {
                    IndexMeta index = tm.getAllIndexes().get(pkIndexName);
                    index.setIndextype(IndexType.PRIMARY);
                }
            }
            if (tm.getAllIndexes().isEmpty()) {
                throw new ShouldNeverHappenException(String.format("Could not found any index in the table: %s", tableName));
            }
        }
        return tm;
    }
}
