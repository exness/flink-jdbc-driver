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

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.table.jdbc.utils.DriverUtils.getHeartBeatMessageActive;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkResultSet}. */
public class FlinkResultSetTest {
    private static final int RECORD_SIZE = 5000;
    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("v1", DataTypes.BOOLEAN()),
                    Column.physical("v2", DataTypes.TINYINT()),
                    Column.physical("v3", DataTypes.SMALLINT()),
                    Column.physical("v4", DataTypes.INT()),
                    Column.physical("v5", DataTypes.BIGINT()),
                    Column.physical("v6", DataTypes.FLOAT()),
                    Column.physical("v7", DataTypes.DOUBLE()),
                    Column.physical("v8", DataTypes.DECIMAL(10, 5)),
                    Column.physical("v9", DataTypes.STRING()),
                    Column.physical("v10", DataTypes.BYTES()),
                    Column.physical(
                            "v11",
                            DataTypes.MAP(
                                    DataTypes.STRING(),
                                    DataTypes.MAP(DataTypes.INT(), DataTypes.BIGINT()))),
                    Column.physical(
                            "v12",
                            DataTypes.ROW(
                                    DataTypes.STRING(),
                                    DataTypes.ROW(DataTypes.STRING(), DataTypes.INT()))));

    private static CloseableIterator<RowData> getRowDataPrimitiveIterator() {
        return CloseableIterator.adapterForIterator(
                IntStream.range(0, RECORD_SIZE)
                        .boxed()
                        .map(
                                v -> {
                                    Map<StringData, MapData> map = new HashMap<>();
                                    Map<Integer, Long> valueMap = new HashMap<>();
                                    valueMap.put(v, v.longValue());
                                    map.put(
                                            StringData.fromString(v.toString()),
                                            new GenericMapData(valueMap));
                                    return (RowData)
                                            GenericRowData.of(
                                                    v % 2 == 0,
                                                    v.byteValue(),
                                                    v.shortValue(),
                                                    v,
                                                    v.longValue(),
                                                    (float) (v + 0.1),
                                                    v + 0.22,
                                                    DecimalData.fromBigDecimal(
                                                            new BigDecimal(v + ".55555"), 10, 5),
                                                    StringData.fromString(v.toString()),
                                                    v.toString().getBytes(),
                                                    new GenericMapData(map),
                                                    GenericRowData.of(
                                                            StringData.fromString(v.toString()),
                                                            GenericRowData.of(
                                                                    StringData.fromString(
                                                                            v + "_nested"),
                                                                    v)));
                                })
                        .iterator());
    }

    private static CloseableIterator<RowData> getRowDataNullIterator() {
        return CloseableIterator.adapterForIterator(
                Collections.singletonList(
                                (RowData)
                                        GenericRowData.of(
                                                null, null, null, null, null, null, null, null,
                                                null, null, null, null))
                        .iterator());
    }

    private static void validateNullResultData(ResultSet resultSet, boolean isRowKindIncluded)
            throws SQLException {
        int offset = isRowKindIncluded ? 1 : 0;
        if (isRowKindIncluded) {
            assertEquals(resultSet.getString(1), RowKind.INSERT.shortString());
            assertFalse(resultSet.wasNull());
        }
        assertFalse(resultSet.getBoolean(1 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(1 + offset));
        assertTrue(resultSet.wasNull());
        assertEquals((byte) 0, resultSet.getByte(2 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(2 + offset));
        assertTrue(resultSet.wasNull());
        assertEquals((short) 0, resultSet.getShort(3 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(3 + offset));
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getInt(4 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(4 + offset));
        assertTrue(resultSet.wasNull());
        assertEquals(0L, resultSet.getLong(5 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(5 + offset));
        assertTrue(resultSet.wasNull());
        assertEquals((float) 0.0, resultSet.getFloat(6 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(6 + offset));
        assertTrue(resultSet.wasNull());
        assertEquals(0.0, resultSet.getDouble(7 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(7 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBigDecimal(8 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(8 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getString(9 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(9 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBytes(10 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(10 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(11 + offset));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getObject(12 + offset));
        assertTrue(resultSet.wasNull());
    }

    private static void validatePrimitiveResultData(
            ResultSet resultSet, boolean isRowKindIncluded, boolean isStreamingStateIncluded)
            throws SQLException {
        int resultCount = 0;
        int offset = (isRowKindIncluded ? 1 : 0) + (isStreamingStateIncluded ? 1 : 0);
        while (resultSet.next()) {

            if (isRowKindIncluded) {
                assertEquals(resultSet.getString(1), RowKind.INSERT.shortString());
                assertFalse(resultSet.wasNull());
            }

            if (isStreamingStateIncluded) {
                assertEquals(resultSet.getString(offset), getHeartBeatMessageActive());
                assertFalse(resultSet.wasNull());
            }

            Integer val = resultSet.getInt("v4");
            assertEquals(val, resultCount);
            resultCount++;

            // Get and validate each column value
            assertEquals(val % 2 == 0, resultSet.getBoolean(1 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val % 2 == 0, resultSet.getBoolean("v1"));
            assertFalse(resultSet.wasNull());
            assertEquals(val % 2 == 0, resultSet.getObject(1 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val % 2 == 0, resultSet.getObject("v1"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.byteValue(), resultSet.getByte(2 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.byteValue(), resultSet.getByte("v2"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.byteValue(), resultSet.getObject(2 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.byteValue(), resultSet.getObject("v2"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.shortValue(), resultSet.getShort(3 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.shortValue(), resultSet.getShort("v3"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.shortValue(), resultSet.getObject(3 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.shortValue(), resultSet.getObject("v3"));
            assertFalse(resultSet.wasNull());
            assertEquals(val, resultSet.getInt(4 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val, resultSet.getInt("v4"));
            assertFalse(resultSet.wasNull());
            assertEquals(val, resultSet.getObject(4 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val, resultSet.getObject("v4"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.longValue(), resultSet.getLong(5 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.longValue(), resultSet.getLong("v5"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.longValue(), resultSet.getObject(5 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.longValue(), resultSet.getObject("v5"));
            assertFalse(resultSet.wasNull());
            assertTrue(resultSet.getFloat(6 + offset) - val - 0.1 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue(resultSet.getFloat("v6") - val - 0.1 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue((float) resultSet.getObject(6 + offset) - val - 0.1 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue((float) resultSet.getObject("v6") - val - 0.1 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue(resultSet.getDouble(7 + offset) - val - 0.22 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue(resultSet.getDouble("v7") - val - 0.22 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue((double) resultSet.getObject(7 + offset) - val - 0.22 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertTrue((double) resultSet.getObject("v7") - val - 0.22 < 0.0001);
            assertFalse(resultSet.wasNull());
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getBigDecimal(8 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getBigDecimal("v8"));
            assertFalse(resultSet.wasNull());
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getObject(8 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getObject("v8"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), resultSet.getString(9 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), resultSet.getString("v9"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), resultSet.getObject(9 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), resultSet.getObject("v9"));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), new String(resultSet.getBytes(10 + offset)));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), new String(resultSet.getBytes("v10")));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), new String((byte[]) resultSet.getObject(10 + offset)));
            assertFalse(resultSet.wasNull());
            assertEquals(val.toString(), new String((byte[]) resultSet.getObject("v10")));
            assertFalse(resultSet.wasNull());

            // Validate map data
            Map<String, Map<Integer, Long>> map = new HashMap<>();
            Map<Integer, Long> valueMap = new HashMap<>();
            valueMap.put(val, val.longValue());
            map.put(String.valueOf(val), valueMap);
            assertEquals(map, resultSet.getObject(11 + offset));
            assertFalse(resultSet.wasNull());
            assertEquals(map, resultSet.getObject("v11"));
            assertFalse(resultSet.wasNull());

            // Validate row data
            assertEquals(
                    "{\"f0\":\""
                            + val
                            + "\",\"f1\":{\"f0\":\""
                            + val
                            + "_nested\",\"f1\":"
                            + val
                            + "}}",
                    resultSet.getObject("v12"));
            // Get data according to wrong data type
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(1 + offset),
                    "java.lang.ClassCastException: java.lang.Boolean cannot be cast to java.lang.Long");
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(6 + offset),
                    "java.lang.ClassCastException: java.lang.Float cannot be cast to java.lang.Long");
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(7 + offset),
                    "java.lang.ClassCastException: java.lang.Double cannot be cast to java.lang.Long");
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(8 + offset),
                    "java.lang.ClassCastException: java.lang.BigDecimal cannot be cast to java.lang.Long");

            // Get not exist column
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong("id1"),
                    "Column[id1] is not exist");
            assertThrows(SQLException.class, () -> resultSet.getLong(12 + offset));
            assertThrowsExactly(
                    SQLException.class, () -> resultSet.getLong(-1), "Column[-1] is not exist");
        }
        assertEquals(resultCount, RECORD_SIZE);
    }

    /* ----------------------------test BatchResultSet------------------------------------------- */
    @Test
    public void testBatchResultSetPrimitiveDataNoRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataPrimitiveIterator();
        try (ResultSet resultSet =
                new FlinkBatchResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        false)) {
            validatePrimitiveResultData(resultSet, false, false);
        }
    }

    @Test
    public void testBatchResultSetPrimitiveDataWithRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataPrimitiveIterator();
        try (ResultSet resultSet =
                new FlinkBatchResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        true)) {
            validatePrimitiveResultData(resultSet, true, false);
        }
    }

    @Test
    public void testStringBatchResultSetNullDataNoRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataNullIterator();
        try (ResultSet resultSet =
                new FlinkBatchResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        false)) {
            assertTrue(resultSet.next());
            validateNullResultData(resultSet, false);
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testStringBatchResultSetNullDataWithRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataNullIterator();
        try (ResultSet resultSet =
                new FlinkBatchResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        true)) {
            assertTrue(resultSet.next());
            validateNullResultData(resultSet, true);
            assertFalse(resultSet.next());
        }
    }

    /* ----------------------------test StreamingResultSet--------------------------------------- */

    @Test
    public void testStreamingResultSetPrimitiveDataNoRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataPrimitiveIterator();
        try (ResultSet resultSet =
                new FlinkStreamingResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        false,
                        -1)) {
            validatePrimitiveResultData(resultSet, false, false);
        }
    }

    @Test
    public void testStreamingResultSetPrimitiveDataRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataPrimitiveIterator();
        try (ResultSet resultSet =
                new FlinkStreamingResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        true,
                        -1)) {
            validatePrimitiveResultData(resultSet, true, false);
        }
    }

    @Test
    public void testStreamingResultSetPrimitiveDataNoRowKindStreamingStateColumnPresent()
            throws Exception {
        CloseableIterator<RowData> data = getRowDataPrimitiveIterator();
        try (ResultSet resultSet =
                new FlinkStreamingResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        false,
                        1000)) {
            validatePrimitiveResultData(resultSet, false, true);
        }
    }

    @Test
    public void testStreamingResultSetPrimitiveDataRowKindStreamingStateColumnPresent()
            throws Exception {
        CloseableIterator<RowData> data = getRowDataPrimitiveIterator();
        try (ResultSet resultSet =
                new FlinkStreamingResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        true,
                        1000)) {
            validatePrimitiveResultData(resultSet, true, true);
        }
    }

    /* ------------------------------------------------------------------------------------------ */

    @Test
    public void testStringStreamingResultSetNullDataNoRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataNullIterator();
        try (ResultSet resultSet =
                new FlinkStreamingResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        false,
                        -1)) {
            assertTrue(resultSet.next());
            validateNullResultData(resultSet, false);
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testStringStreamingResultSetNullDataWithRowKind() throws Exception {
        CloseableIterator<RowData> data = getRowDataNullIterator();
        try (ResultSet resultSet =
                new FlinkStreamingResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()),
                        true,
                        -1)) {
            assertTrue(resultSet.next());
            validateNullResultData(resultSet, true);
            assertFalse(resultSet.next());
        }
    }
}
