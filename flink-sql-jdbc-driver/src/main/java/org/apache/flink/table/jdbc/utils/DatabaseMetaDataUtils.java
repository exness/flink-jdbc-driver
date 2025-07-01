/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.jdbc.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.jdbc.FlinkBatchResultSet;
import org.apache.flink.table.jdbc.FlinkDatabaseMetaData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;

/** Utils to create catalog/schema results for {@link FlinkDatabaseMetaData}. */
public class DatabaseMetaDataUtils {
    private static final Column TABLE_CAT_COLUMN =
            Column.physical("TABLE_CAT", DataTypes.STRING().notNull());
    private static final Column TABLE_SCHEM_COLUMN =
            Column.physical("TABLE_SCHEM", DataTypes.STRING().notNull());
    private static final Column TABLE_CATALOG_COLUMN =
            Column.physical("TABLE_CATALOG", DataTypes.STRING());
    private static final Column TABLE_TYPE_COLUMN =
            Column.physical("TABLE_TYPE", DataTypes.STRING());
    private static final Column TABLE_NAME_COLUMN =
            Column.physical("TABLE_NAME", DataTypes.STRING());
    private static final Column TYPE_CAT_COLUMN = Column.physical("TYPE_CAT", DataTypes.STRING());
    private static final Column TYPE_SCHEM_COLUMN =
            Column.physical("TYPE_SCHEM", DataTypes.STRING());
    private static final Column TYPE_NAME_COLUMN = Column.physical("TYPE_NAME", DataTypes.STRING());
    private static final Column SELF_REFERENCING_COL_NAME_COLUMN =
            Column.physical("SELF_REFERENCING_COL_NAME", DataTypes.STRING());
    private static final Column REF_GENERATION_COLUMN =
            Column.physical("REF_GENERATION", DataTypes.STRING());
    private static final Column COLUMN_NAME_COLUMN =
            Column.physical("COLUMN_NAME", DataTypes.STRING());
    private static final Column DATA_TYPE_COLUMN = Column.physical("DATA_TYPE", DataTypes.INT());
    private static final Column COLUMN_SIZE_COLUMN =
            Column.physical("COLUMN_SIZE", DataTypes.INT());
    private static final Column BUFFER_LENGTH_COLUMN =
            Column.physical("BUFFER_LENGTH", DataTypes.INT());
    private static final Column DECIMAL_DIGITS_COLUMN =
            Column.physical("DECIMAL_DIGITS", DataTypes.INT());
    private static final Column NUM_PREC_RADIX_COLUMN =
            Column.physical("NUM_PREC_RADIX", DataTypes.INT());
    private static final Column NULLABLE_COLUMN = Column.physical("NULLABLE", DataTypes.INT());
    private static final Column REMARKS_COLUMN = Column.physical("REMARKS", DataTypes.STRING());
    private static final Column COLUMN_DEF_COLUMN =
            Column.physical("COLUMN_DEF", DataTypes.STRING());
    private static final Column SQL_DATA_TYPE_COLUMN =
            Column.physical("SQL_DATA_TYPE", DataTypes.INT());
    private static final Column SQL_DATETIME_SUB_COLUMN =
            Column.physical("SQL_DATETIME_SUB", DataTypes.INT());
    private static final Column CHAR_OCTET_LENGTH_COLUMN =
            Column.physical("CHAR_OCTET_LENGTH", DataTypes.INT());
    private static final Column ORDINAL_POSITION_COLUMN =
            Column.physical("ORDINAL_POSITION", DataTypes.INT());
    private static final Column IS_NULLABLE_COLUMN =
            Column.physical("IS_NULLABLE", DataTypes.STRING());
    private static final Column SCOPE_CATALOG_COLUMN =
            Column.physical("SCOPE_CATALOG", DataTypes.STRING());
    private static final Column SCOPE_SCHEMA_COLUMN =
            Column.physical("SCOPE_SCHEMA", DataTypes.STRING());
    private static final Column SCOPE_TABLE_COLUMN =
            Column.physical("SCOPE_TABLE", DataTypes.STRING());
    private static final Column SOURCE_DATA_TYPE_COLUMN =
            Column.physical("SOURCE_DATA_TYPE", DataTypes.INT());
    private static final Column IS_AUTOINCREMENT_COLUMN =
            Column.physical("IS_AUTOINCREMENT", DataTypes.STRING());
    private static final Column IS_GENERATEDCOLUMN_COLUMN =
            Column.physical("IS_GENERATEDCOLUMN", DataTypes.STRING());
    private static final Column KEY_SEQ_COLUMN = Column.physical("KEY_SEQ", DataTypes.STRING());
    private static final Column PK_NAME_COLUMN = Column.physical("PK_NAME", DataTypes.STRING());

    /**
     * Create result set for catalogs. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_CAT String => catalog name.
     * </ul>
     *
     * <p>The results are ordered by catalog name.
     *
     * @param statement The statement for database meta data
     * @param result The result for catalogs
     * @return a ResultSet object in which each row has a single String column that is a catalog
     *     name
     */
    public static FlinkBatchResultSet createCatalogsResultSet(
            Statement statement, StatementResult result) {
        List<RowData> catalogs = new ArrayList<>();
        result.forEachRemaining(catalogs::add);
        catalogs.sort(Comparator.comparing(v -> v.getString(0)));

        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(catalogs.iterator()),
                ResolvedSchema.of(TABLE_CAT_COLUMN),
                false);
    }

    /**
     * Create result set for schemas. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_SCHEM String => schema name
     *   <li>TABLE_CATALOG String => catalog name (may be null)
     * </ul>
     *
     * <p>The results are ordered by TABLE_CATALOG and TABLE_SCHEM.
     *
     * @param statement The statement for database meta data
     * @param catalogs The catalog list
     * @param catalogSchemas The catalog with schema list
     * @return a ResultSet object in which each row is a schema description
     */
    public static FlinkBatchResultSet createSchemasResultSet(
            Statement statement, List<String> catalogs, Map<String, List<String>> catalogSchemas) {
        List<RowData> schemaWithCatalogList = new ArrayList<>();
        List<String> catalogList = new ArrayList<>(catalogs);
        catalogList.sort(String::compareTo);
        for (String catalog : catalogList) {
            List<String> schemas = catalogSchemas.get(catalog);
            schemas.sort(String::compareTo);
            schemas.forEach(
                    s ->
                            schemaWithCatalogList.add(
                                    GenericRowData.of(
                                            StringData.fromString(s),
                                            StringData.fromString(catalog))));
        }

        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(schemaWithCatalogList.iterator()),
                ResolvedSchema.of(TABLE_SCHEM_COLUMN, TABLE_CATALOG_COLUMN),
                false);
    }

    /**
     * Create result set for table types. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_TYPE_COLUMN String => table type
     * </ul>
     *
     * <p>Currently only TABLE type is supported.
     *
     * @param statement The statement for database meta data
     * @return a ResultSet object in which each row is a table type
     */
    public static FlinkBatchResultSet createTableTypesResultSet(Statement statement) {
        List<RowData> tableTypes = new ArrayList<>();
        GenericRowData row = new GenericRowData(RowKind.INSERT, 1);
        row.setField(0, StringData.fromString("TABLE"));
        tableTypes.add(row);
        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(tableTypes.iterator()),
                ResolvedSchema.of(TABLE_TYPE_COLUMN),
                false);
    }

    /**
     * Create a result set for tables in a catalog. The results are ordered by TABLE_TYPE,
     * TABLE_CAT, TABLE_SCHEM and TABLE_NAME. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_CAT String => table catalog (may be null)
     *   <li>TABLE_SCHEM String => table schema (may be null)
     *   <li>TABLE_NAME String => table name
     *   <li>TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
     *       "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *   <li>REMARKS String => explanatory comment on the table (may be null)
     *   <li>TYPE_CAT String => the types catalog (may be null)
     *   <li>TYPE_SCHEM String => the types schema (may be null)
     *   <li>TYPE_NAME String => type name (may be null)
     *   <li>SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a
     *       typed table (may be null)
     *   <li>REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created.
     *       Values are "SYSTEM", "USER", "DERIVED". (may be null)
     * </ul>
     *
     * @param statement The statement for database meta data
     * @param result Statement result
     * @param catalog Catalog name
     * @param schema Schema name
     * @return a ResultSet object in which each row is a table
     */
    public static FlinkBatchResultSet createTablesResultSet(
            Statement statement, StatementResult result, String catalog, String schema) {
        List<RowData> tables = new ArrayList<>();
        result.forEachRemaining(
                row -> {
                    final GenericRowData enrichedRow = new GenericRowData(10);
                    enrichedRow.setField(0, StringData.fromString(catalog));
                    enrichedRow.setField(1, StringData.fromString(schema));
                    enrichedRow.setField(2, row.getString(0));
                    enrichedRow.setField(3, StringData.fromString("TABLE"));
                    enrichedRow.setField(4, null);
                    enrichedRow.setField(5, null);
                    enrichedRow.setField(6, null);
                    enrichedRow.setField(7, null);
                    enrichedRow.setField(8, null);
                    enrichedRow.setField(9, null);
                    tables.add(enrichedRow);
                });
        tables.sort(
                Comparator.comparing((RowData v) -> v.getString(3)) // TABLE_TYPE
                        .thenComparing(v -> v.getString(0)) // TABLE_CAT
                        .thenComparing(v -> v.getString(1)) // TABLE_SCHEM
                        .thenComparing(v -> v.getString(2)) // TABLE_NAME
                );
        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(tables.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        TABLE_TYPE_COLUMN,
                        TYPE_CAT_COLUMN,
                        TYPE_SCHEM_COLUMN,
                        TYPE_NAME_COLUMN,
                        SELF_REFERENCING_COL_NAME_COLUMN,
                        REF_GENERATION_COLUMN),
                false);
    }

    /**
     * Retrieves the schema names available in this database. The results are ordered by <code>
     * TABLE_CATALOG</code> and <code>TABLE_SCHEM</code>.
     *
     * <p>The schema columns are:
     *
     * <OL>
     *   <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
     *   <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be <code>null</code>)
     * </OL>
     */
    public static FlinkBatchResultSet createSchemasResultSet(
            Statement statement, StatementResult result, String catalog) {
        List<RowData> databases = new ArrayList<>();
        result.forEachRemaining(
                row -> {
                    final GenericRowData enrichedRow = new GenericRowData(2);
                    enrichedRow.setField(0, row.getString(0));
                    enrichedRow.setField(1, StringData.fromString(catalog));
                    databases.add(enrichedRow);
                });
        databases.sort(
                Comparator.comparing((RowData v) -> v.getString(1)) // TABLE_CATALOG
                        .thenComparing(v -> v.getString(0)) // TABLE_SCHEM
                );
        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(databases.iterator()),
                ResolvedSchema.of(TABLE_SCHEM_COLUMN, TABLE_CAT_COLUMN),
                false);
    }

    /**
     * Create a result set for columns in a table. They are ordered by TABLE_CAT,TABLE_SCHEM,
     * TABLE_NAME, and ORDINAL_POSITION. The schema columns are:
     *
     * <OL>
     *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *   <LI><B>TABLE_NAME</B> String {@code =>} table name
     *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *   <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     *   <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name, for a UDT the type
     *       name is fully qualified
     *   <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     *   <LI><B>BUFFER_LENGTH</B> is not used.
     *   <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
     *       for data types where DECIMAL_DIGITS is not applicable.
     *   <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     *   <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
     *       <UL>
     *         <LI>columnNoNulls - might not allow <code>NULL</code> values
     *         <LI>columnNullable - definitely allows <code>NULL</code> values
     *         <LI>columnNullableUnknown - nullability unknown
     *       </UL>
     *   <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
     *   <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be
     *       interpreted as a string when the value is enclosed in single quotes (may be <code>null
     *       </code>)
     *   <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     *   <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     *   <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in
     *       the column
     *   <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table (starting at 1)
     *   <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability
     *       for a column.
     *       <UL>
     *         <LI>YES --- if the column can include NULLs
     *         <LI>NO --- if the column cannot include NULLs
     *         <LI>empty string --- if the nullability for the column is unknown
     *       </UL>
     *   <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope of a
     *       reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     *   <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope of a reference
     *       attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *   <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope of a reference
     *       attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *   <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or
     *       user-generated Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
     *       isn't DISTINCT or user-generated REF)
     *   <LI><B>IS_AUTOINCREMENT</B> String {@code =>} Indicates whether this column is auto
     *       incremented
     *       <UL>
     *         <LI>YES --- if the column is auto incremented
     *         <LI>NO --- if the column is not auto incremented
     *         <LI>empty string --- if it cannot be determined whether the column is auto
     *             incremented
     *       </UL>
     *   <LI><B>IS_GENERATEDCOLUMN</B> String {@code =>} Indicates whether this is a generated
     *       column
     *       <UL>
     *         <LI>YES --- if this a generated column
     *         <LI>NO --- if this not a generated column
     *         <LI>empty string --- if it cannot be determined whether this is a generated column
     *       </UL>
     * </OL>
     */
    public static ResultSet createColumnsResultSet(
            Statement statement,
            StatementResult result,
            String catalog,
            String schema,
            String table) {
        List<RowData> tables = new ArrayList<>();
        AtomicInteger ordinal = new AtomicInteger(1);
        result.forEachRemaining(
                row -> {
                    final GenericRowData enrichedRow = new GenericRowData(24);

                    final LogicalType columnType =
                            LogicalTypeParser.parse(
                                    row.getString(1).toString(),
                                    Thread.currentThread().getContextClassLoader());

                    enrichedRow.setField(0, StringData.fromString(catalog));
                    enrichedRow.setField(1, StringData.fromString(schema));
                    enrichedRow.setField(2, StringData.fromString(table));
                    enrichedRow.setField(3, row.getString(0)); // column name
                    enrichedRow.setField(4, getSqlType(columnType)); // data type
                    enrichedRow.setField(5, row.getString(1)); // type name
                    enrichedRow.setField(6, getColumnSize(columnType)); // column size
                    enrichedRow.setField(7, null); // buffer length
                    enrichedRow.setField(8, getDecimalDigits(columnType)); // decimal digits
                    enrichedRow.setField(9, getNumPrecRadix(columnType)); // num prec radix
                    enrichedRow.setField(
                            10, row.getBoolean(2) ? columnNullable : columnNoNulls); // nullable
                    enrichedRow.setField(11, null); // remarks
                    enrichedRow.setField(12, null); // column def
                    enrichedRow.setField(13, null); // sql data type
                    enrichedRow.setField(14, null); // sql datetime sub
                    enrichedRow.setField(15, null); // char octet length
                    enrichedRow.setField(16, ordinal.getAndIncrement()); // ordinal position
                    enrichedRow.setField(
                            17,
                            StringData.fromString(row.getBoolean(2) ? "YES" : "NO")); // is nullable
                    enrichedRow.setField(18, null); // scope catalog
                    enrichedRow.setField(19, null); // scope schema
                    enrichedRow.setField(20, null); // scope table
                    enrichedRow.setField(21, null); // source data type
                    enrichedRow.setField(22, StringData.fromString("NO")); // is auto increment
                    enrichedRow.setField(23, StringData.fromString("")); // is generated column
                    tables.add(enrichedRow);
                });
        tables.sort(
                Comparator.comparing((RowData v) -> v.getString(0)) // TABLE_CAT
                        .thenComparing(v -> v.getString(1)) // TABLE_SCHEM
                        .thenComparing(v -> v.getString(2)) // TABLE_NAME
                        .thenComparingInt(v -> v.getInt(16)) // ORDINAL_POSITION
                );
        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(tables.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        COLUMN_NAME_COLUMN,
                        DATA_TYPE_COLUMN,
                        TYPE_NAME_COLUMN,
                        COLUMN_SIZE_COLUMN,
                        BUFFER_LENGTH_COLUMN,
                        DECIMAL_DIGITS_COLUMN,
                        NUM_PREC_RADIX_COLUMN,
                        NULLABLE_COLUMN,
                        REMARKS_COLUMN,
                        COLUMN_DEF_COLUMN,
                        SQL_DATA_TYPE_COLUMN,
                        SQL_DATETIME_SUB_COLUMN,
                        CHAR_OCTET_LENGTH_COLUMN,
                        ORDINAL_POSITION_COLUMN,
                        IS_NULLABLE_COLUMN,
                        SCOPE_CATALOG_COLUMN,
                        SCOPE_SCHEMA_COLUMN,
                        SCOPE_TABLE_COLUMN,
                        SOURCE_DATA_TYPE_COLUMN,
                        IS_AUTOINCREMENT_COLUMN,
                        IS_GENERATEDCOLUMN_COLUMN),
                false);
    }

    public static ResultSet createEmptyColumnsResultSet(Statement statement) {

        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.empty(),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        COLUMN_NAME_COLUMN,
                        DATA_TYPE_COLUMN,
                        TYPE_NAME_COLUMN,
                        COLUMN_SIZE_COLUMN,
                        BUFFER_LENGTH_COLUMN,
                        DECIMAL_DIGITS_COLUMN,
                        NUM_PREC_RADIX_COLUMN,
                        NULLABLE_COLUMN,
                        REMARKS_COLUMN,
                        COLUMN_DEF_COLUMN,
                        SQL_DATA_TYPE_COLUMN,
                        SQL_DATETIME_SUB_COLUMN,
                        CHAR_OCTET_LENGTH_COLUMN,
                        ORDINAL_POSITION_COLUMN,
                        IS_NULLABLE_COLUMN,
                        SCOPE_CATALOG_COLUMN,
                        SCOPE_SCHEMA_COLUMN,
                        SCOPE_TABLE_COLUMN,
                        SOURCE_DATA_TYPE_COLUMN,
                        IS_AUTOINCREMENT_COLUMN,
                        IS_GENERATEDCOLUMN_COLUMN),
                false);
    }

    /**
     * Retrieves a description of the given table's primary key columns. They are ordered by
     * COLUMN_NAME.
     *
     * <p>Each primary key column description has the following columns:
     *
     * <OL>
     *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *   <LI><B>TABLE_NAME</B> String {@code =>} table name
     *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *   <LI><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value of 1
     *       represents the first column of the primary key, a value of 2 would represent the second
     *       column within the primary key).
     *   <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
     * </OL>
     */
    public static ResultSet createPrimaryKeysResultSet(
            Statement statement, String catalog, String schema, String table) {
        List<RowData> pKeys = new ArrayList<>();
        // todo: actually get primary keys. Currently we return an empty result set allow
        // introspection from DataGrip.
        return new FlinkBatchResultSet(
                statement,
                CloseableIterator.adapterForIterator(pKeys.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        COLUMN_NAME_COLUMN,
                        KEY_SEQ_COLUMN,
                        PK_NAME_COLUMN),
                false);
    }

    private static int getSqlType(LogicalType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return java.sql.Types.BOOLEAN;
            case TINYINT:
                return java.sql.Types.TINYINT;
            case SMALLINT:
                return java.sql.Types.SMALLINT;
            case INTEGER:
                return java.sql.Types.INTEGER;
            case BIGINT:
                return java.sql.Types.BIGINT;
            case FLOAT:
                return java.sql.Types.FLOAT;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case BINARY:
                return java.sql.Types.BINARY;
            case VARBINARY:
                return java.sql.Types.VARBINARY;
            case CHAR:
            case VARCHAR:
                return java.sql.Types.VARCHAR;
            case DECIMAL:
                return java.sql.Types.DECIMAL;
            case DATE:
                return java.sql.Types.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return java.sql.Types.TIME;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return java.sql.Types.TIMESTAMP;
            case ARRAY:
                return java.sql.Types.ARRAY;
            case MAP:
                return java.sql.Types.JAVA_OBJECT;
            case ROW:
                return java.sql.Types.STRUCT;
            default:
                return java.sql.Types.OTHER;
        }
    }

    private static Integer getColumnSize(LogicalType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case CHAR:
                return ((CharType) dataType).getLength();
            case VARCHAR:
                return ((VarCharType) dataType).getLength();
            case BINARY:
                return ((BinaryType) dataType).getLength();
            case VARBINARY:
                return ((VarBinaryType) dataType).getLength();
            case DECIMAL:
                return ((DecimalType) dataType).getPrecision();
            case DATE:
                return 10;
            case TIME_WITHOUT_TIME_ZONE:
                return 8;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return 26;
            default:
                return null;
        }
    }

    private static Integer getDecimalDigits(LogicalType dataType) {
        switch (dataType.getTypeRoot()) {
            case DECIMAL:
                return ((DecimalType) dataType).getScale();
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            default:
                return null;
        }
    }

    private static Integer getNumPrecRadix(LogicalType dataType) {
        switch (dataType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return 10;
            case FLOAT:
            case DOUBLE:
                return 2;
            default:
                return null;
        }
    }
}
