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

package org.apache.flink.table.jdbc;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.jdbc.utils.DriverUtils.checkNotNull;
import static org.apache.flink.table.jdbc.utils.DriverUtils.generateRowKindColumnName;
import static org.apache.flink.table.jdbc.utils.DriverUtils.generateStreamStateColumnName;
import static org.apache.flink.table.jdbc.utils.DriverUtils.getHeartBeatMessageActive;
import static org.apache.flink.table.jdbc.utils.DriverUtils.getHeartBeatMessageIdle;

/** ResultSet for flink jdbc driver. */
abstract class FlinkResultSet extends BaseResultSet {
    protected final List<DataType> enrichedDataTypeList;
    protected final List<String> columnNameList;
    protected final Statement statement;
    protected final boolean asChangeLog;
    protected final boolean addStreamStateColumn;
    private final List<RowData.FieldGetter> fieldGetterList;
    private final FlinkResultSetMetaData resultSetMetaData;
    private final Map<Integer, JsonRowDataSerializationSchema> jsonSerializers = new HashMap<>();
    protected final List<DataType> dataTypeList;
    protected RowData currentRow;
    protected boolean wasNull;
    protected volatile boolean closed;

    public FlinkResultSet(
            Statement statement,
            ResolvedSchema schema,
            boolean asChangeLog,
            boolean addStreamStateColumn) {
        this.statement = checkNotNull(statement, "Statement cannot be null");
        this.asChangeLog = asChangeLog;
        this.addStreamStateColumn = addStreamStateColumn;
        this.currentRow = null;
        this.wasNull = false;
        this.dataTypeList = schema.getColumnDataTypes();
        this.enrichedDataTypeList = createDataTypeList(schema.getColumnDataTypes());
        this.columnNameList = createColumnNameList(schema.getColumnNames());
        this.fieldGetterList = createFieldGetterList(enrichedDataTypeList);
        this.resultSetMetaData = new FlinkResultSetMetaData(columnNameList, enrichedDataTypeList);
    }

    protected RowData enrich(RowData row, boolean isHeartBeatRecord) throws SQLException {
        if (row == null) {
            return null;
        }
        if (!asChangeLog && !addStreamStateColumn) {
            return row;
        }

        if (!(row instanceof GenericRowData)) {
            throw new SQLException(
                    "Expected GenericRowData but got " + row.getClass().getSimpleName());
        }

        final int originalArity = row.getArity();
        final int addedFieldCount = (asChangeLog ? 1 : 0) + (addStreamStateColumn ? 1 : 0);
        final GenericRowData newRow =
                new GenericRowData(row.getRowKind(), originalArity + addedFieldCount);

        int fieldPosition = 0;

        // Add row kind metadata
        if (asChangeLog) {
            newRow.setField(fieldPosition++, StringData.fromString(row.getRowKind().shortString()));
        }

        // Add stream state message
        if (addStreamStateColumn) {
            if (isHeartBeatRecord) {
                newRow.setField(fieldPosition++, StringData.fromString(getHeartBeatMessageIdle()));
            } else {
                newRow.setField(
                        fieldPosition++, StringData.fromString(getHeartBeatMessageActive()));
            }
        }

        // Copy original fields
        final GenericRowData sourceRow = (GenericRowData) row;
        for (int i = 0; i < originalArity; i++) {
            newRow.setField(fieldPosition + i, sourceRow.getField(i));
        }

        return newRow;
    }

    private List<DataType> createDataTypeList(List<DataType> originalDataTypes) {
        List<DataType> dataTypes = new ArrayList<>();

        if (asChangeLog) {
            dataTypes.add(DataTypes.STRING()); // Row kind column
        }
        if (addStreamStateColumn) {
            dataTypes.add(DataTypes.STRING()); // Idleness status column
        }

        dataTypes.addAll(originalDataTypes);
        return dataTypes;
    }

    private List<String> createColumnNameList(List<String> originalColumnNames) {
        List<String> columnNames = new ArrayList<>();

        if (asChangeLog) {
            columnNames.add(generateRowKindColumnName());
        }
        if (addStreamStateColumn) {
            columnNames.add(generateStreamStateColumnName());
        }

        columnNames.addAll(originalColumnNames);
        return columnNames;
    }

    private List<RowData.FieldGetter> createFieldGetterList(List<DataType> dataTypeList) {
        List<RowData.FieldGetter> fieldGetterList = new ArrayList<>(dataTypeList.size());
        for (int i = 0; i < dataTypeList.size(); i++) {
            fieldGetterList.add(RowData.createFieldGetter(dataTypeList.get(i).getLogicalType(), i));
        }

        return fieldGetterList;
    }

    @Override
    public abstract boolean next() throws SQLException;

    @Override
    public abstract void close() throws SQLException;

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("This result set is already closed");
        }
    }

    private void checkValidRow() throws SQLException {
        if (currentRow == null) {
            throw new SQLException("Not on a valid row");
        }
        if (currentRow.getArity() <= 0) {
            throw new SQLException("Empty row with no data");
        }
    }

    private void checkValidColumn(int columnIndex) throws SQLException {
        if (columnIndex <= 0) {
            throw new SQLException(
                    String.format("Column index[%s] must be positive.", columnIndex));
        }

        final int columnCount = currentRow.getArity();
        if (columnIndex > columnCount) {
            throw new SQLException(
                    String.format(
                            "Column index %s out of bound. There are only %s columns.",
                            columnIndex, columnCount));
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        // fetch size is ignored
        checkClosed();
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException("Rows is negative");
        }
        // fetch size is ignored
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        StringData stringData = currentRow.getString(columnIndex - 1);
        try {
            return wasNull ? null : stringData.toString();
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return !wasNull && currentRow.getBoolean(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? 0 : currentRow.getByte(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? 0 : currentRow.getShort(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? 0 : currentRow.getInt(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? 0L : currentRow.getLong(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? 0 : currentRow.getFloat(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? 0 : currentRow.getDouble(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex).setScale(scale, RoundingMode.HALF_EVEN);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull ? null : currentRow.getBinary(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) getObject(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) getObject(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getColumnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(getColumnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(getColumnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getColumnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(getColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(getColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(getColumnIndex(columnLabel));
    }

    private int getColumnIndex(String columnLabel) throws SQLException {
        int columnIndex = columnNameList.indexOf(columnLabel) + 1;
        if (columnIndex <= 0) {
            throw new SQLDataException(String.format("Column[%s] is not exist", columnLabel));
        }
        return columnIndex;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            DataType dataType = enrichedDataTypeList.get(columnIndex - 1);
            Object object = fieldGetterList.get(columnIndex - 1).getFieldOrNull(currentRow);
            return convertToJavaObject(object, dataType.getLogicalType(), columnIndex);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    private JsonRowDataSerializationSchema getJsonSerializer(RowType rowType, int columnIndex)
            throws SQLException {
        if (jsonSerializers.get(columnIndex) == null) {
            JsonRowDataSerializationSchema jsonSerializer =
                    new JsonRowDataSerializationSchema(
                            rowType,
                            TimestampFormat.ISO_8601,
                            JsonFormatOptions.MapNullKeyMode.LITERAL,
                            "null",
                            false,
                            false);
            try {
                jsonSerializer.open(null);
            } catch (Exception e) {
                throw new SQLException(e);
            }
            jsonSerializers.put(columnIndex, jsonSerializer);
        }
        return jsonSerializers.get(columnIndex);
    }

    private Object convertToJavaObject(Object object, LogicalType dataType, int columnIndex)
            throws SQLException {
        if (object == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                {
                    return object;
                }
            case VARCHAR:
            case CHAR:
                {
                    return object.toString();
                }
            case ROW:
                {
                    byte[] jsonBytes =
                            getJsonSerializer((RowType) dataType, columnIndex)
                                    .serialize((RowData) object);
                    return new String(jsonBytes, StandardCharsets.UTF_8);
                }
            case DECIMAL:
                {
                    return ((DecimalData) object).toBigDecimal();
                }
            case ARRAY:
                {
                    LogicalType elementType = ((ArrayType) dataType).getElementType();
                    ArrayData.ElementGetter elementGetter =
                            ArrayData.createElementGetter(elementType);
                    ArrayData arrayData = (ArrayData) object;
                    int size = arrayData.size();
                    ArrayList<Object> arrayResult = new ArrayList<>(arrayData.size());
                    for (int i = 0; i < size; i++) {
                        arrayResult.add(
                                convertToJavaObject(
                                        elementGetter.getElementOrNull(arrayData, i),
                                        elementType,
                                        columnIndex));
                    }
                    return arrayResult;
                }
            case MAP:
                {
                    LogicalType keyType = ((MapType) dataType).getKeyType();
                    LogicalType valueType = ((MapType) dataType).getValueType();
                    ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
                    ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                    MapData mapData = (MapData) object;
                    int size = mapData.size();
                    ArrayData keyArrayData = mapData.keyArray();
                    ArrayData valueArrayData = mapData.valueArray();
                    Map<Object, Object> mapResult = new HashMap<>();
                    for (int i = 0; i < size; i++) {
                        mapResult.put(
                                convertToJavaObject(
                                        keyGetter.getElementOrNull(keyArrayData, i),
                                        keyType,
                                        columnIndex),
                                convertToJavaObject(
                                        valueGetter.getElementOrNull(valueArrayData, i),
                                        valueType,
                                        columnIndex));
                    }
                    return mapResult;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    return ((TimestampData) object).toTimestamp();
                }
            case TIMESTAMP_WITH_TIME_ZONE:
                {
                    // TODO should be supported after
                    // https://issues.apache.org/jira/browse/FLINK-20869
                    throw new SQLDataException(
                            "TIMESTAMP WITH TIME ZONE is not supported, use TIMESTAMP or TIMESTAMP_LTZ instead");
                }
            case TIME_WITHOUT_TIME_ZONE:
                {
                    return Time.valueOf(
                            LocalTime.ofNanoOfDay(((Number) object).intValue() * 1_000_000L));
                }
            case DATE:
                {
                    return Date.valueOf(LocalDate.ofEpochDay(((Number) object).intValue()));
                }
            default:
                {
                    throw new SQLDataException(
                            String.format("Not supported value type %s", dataType));
                }
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getColumnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getColumnIndex(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);

        DataType dataType = enrichedDataTypeList.get(columnIndex - 1);
        if (!(dataType.getLogicalType() instanceof DecimalType)) {
            throw new SQLException(
                    String.format(
                            "Invalid data type, expect %s but was %s",
                            DecimalType.class.getSimpleName(),
                            dataType.getLogicalType().getClass().getSimpleName()));
        }
        DecimalType decimalType = (DecimalType) dataType.getLogicalType();
        wasNull = currentRow.isNullAt(columnIndex - 1);
        try {
            return wasNull
                    ? null
                    : currentRow
                            .getDecimal(
                                    columnIndex - 1,
                                    decimalType.getPrecision(),
                                    decimalType.getScale())
                            .toBigDecimal();
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel));
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return (Array) getObject(columnIndex);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO get date with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        // TODO get date with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO get time with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        // TODO get time with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO get timestamp with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        // TODO get timestamp with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }
}
