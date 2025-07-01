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

import org.apache.flink.table.data.TimestampData;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

import static org.apache.flink.table.jdbc.FlinkDriverOptions.RESULT_MODE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink statement. */
public class FlinkStatementStreamingDefaultValueTest extends FlinkJdbcDriverTestBase {
    @Test
    public void testResultSetDefaultValues() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(RESULT_MODE.key(), ResultMode.TABLE.name());
        properties.setProperty("execution.runtime-mode", "STREAMING");

        try (FlinkConnection connection = new FlinkConnection(getDriverUri(properties))) {
            try (Statement statement = connection.createStatement()) {

                statement.execute(
                        "CREATE TABLE test_table ("
                                + "bool BOOLEAN NOT NULL, "
                                + "num SMALLINT NOT NULL, "
                                + "dbl DOUBLE NOT NULL, "
                                + "bin BINARY NOT NULL, "
                                + "vbin VARBINARY NOT NULL, "
                                + "chr CHAR NOT NULL, "
                                + "vchr VARCHAR NOT NULL, "
                                + "tmstp TIMESTAMP_LTZ(3) NOT NULL, "
                                + "dte DATE NOT NULL, "
                                + "dcml DECIMAL NOT NULL ,"
                                + "tint TINYINT NOT NULL, "
                                + "sint SMALLINT NOT NULL,"
                                + "nint INTEGER NOT NULL, "
                                + "twotz TIME WITHOUT TIME ZONE NOT NULL, "
                                + "bgint BIGINT NOT NULL, "
                                + "flt FLOAT NOT NULL, "
                                + "tmstpwotz TIMESTAMP NOT NULL,"
                                + "`row` ROW<int1 INTEGER NOT NULL, int2 INTEGER, str1 STRING NOT NULL, nested_row_not_null ROW<str2 STRING, int4 INT NOT NULL> NOT NULL, nested_row_null ROW<int5 int not null>> NOT NULL, "
                                + "`map` MAP<STRING, INT> NOT NULL"
                                // TODO "`array` ARRAY<INT> NOT NULL"
                                // TODO ms "MULTISET<INT NOT NULL> NOT NULL"
                                + ") WITH ("
                                + "'connector' = 'datagen', "
                                + "'rows-per-second' = '1'"
                                + ")");

                statement.execute("SET 'jdbc.streaming.result.heartbeat.interval.ms' = '1000'");

                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_table")) {
                    while (resultSet.next()) {
                        if (resultSet.getString(1).toLowerCase().contains("no new data")) {
                            break;
                        }
                    }

                    assertFalse(resultSet.getBoolean("bool"));
                    assertEquals(0, resultSet.getInt("num"));
                    assertEquals(0.0, resultSet.getDouble("dbl"));
                    assertEquals("", resultSet.getString("chr"));
                    assertEquals("", resultSet.getString("vchr"));
                    assertArrayEquals(new byte[0], resultSet.getBytes("bin"));
                    assertArrayEquals(new byte[0], resultSet.getBytes("vbin"));
                    assertEquals(new BigDecimal("0.0"), resultSet.getBigDecimal("dcml"));
                    assertEquals(0, resultSet.getInt("tint"));
                    assertEquals(0, resultSet.getInt("sint"));
                    assertEquals(0, resultSet.getInt("nint"));
                    assertEquals(0, resultSet.getInt("dte"));
                    assertEquals(0, resultSet.getInt("twotz"));
                    assertEquals(
                            TimestampData.fromEpochMillis(0).toTimestamp(),
                            resultSet.getTimestamp("tmstp"));
                    assertEquals(0L, resultSet.getLong("bgint"));
                    assertEquals(0, resultSet.getFloat("flt"));
                    assertEquals(0, resultSet.getInt("tmstpwotz"));
                    assertEquals(Collections.emptyMap(), resultSet.getObject("map"));
                    assertEquals(
                            "{\"int1\":0,\"int2\":null,\"str1\":\"\",\"nested_row_not_null\":{\"str2\":null,\"int4\":0},\"nested_row_null\":null}",
                            resultSet.getObject("row"));
                }

                assertTrue(statement.execute("SHOW JOBS"));
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertInstanceOf(FlinkStreamingResultSet.class, resultSet);
                    int count = 0;
                    while (resultSet.next()) {
                        count++;
                    }
                    assertEquals(1, count);
                }
            }
        }
    }
}
