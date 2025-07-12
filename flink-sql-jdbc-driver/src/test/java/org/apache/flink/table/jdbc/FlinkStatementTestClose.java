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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink statement. */
public class FlinkStatementTestClose extends FlinkJdbcDriverTestBase {
    @TempDir private Path tempDir;

    @Test
    public void testCloseAllStatements() throws Exception {
        FlinkConnection connection = new FlinkConnection(getDriverUri());
        Statement statement1 = connection.createStatement();
        Statement statement2 = connection.createStatement();
        Statement statement3 = connection.createStatement();

        statement1.close();
        connection.close();

        assertTrue(statement1.isClosed());
        assertTrue(statement2.isClosed());
        assertTrue(statement3.isClosed());
    }

    @Test
    public void testCloseNonQuery() throws Exception {
        CompletableFuture<Void> closedFuture = new CompletableFuture<>();
        try (FlinkConnection connection = new FlinkConnection(new TestingExecutor(closedFuture))) {
            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.executeQuery("INSERT"))
                        .hasMessage(String.format("Statement[%s] is not a query.", "INSERT"));
                closedFuture.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /** Testing executor. */
    static class TestingExecutor implements Executor {
        private final CompletableFuture<Void> closedFuture;

        TestingExecutor(CompletableFuture<Void> closedFuture) {
            this.closedFuture = closedFuture;
        }

        @Override
        public void configureSession(String statement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadableConfig getSessionConfig() {
            HashMap<String, String> config = new HashMap<>();
            return Configuration.fromMap(config);
        }

        @Override
        public Map<String, String> getSessionConfigMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StatementResult executeStatement(String statement) {
            return new StatementResult(
                    null,
                    new CloseableIterator<RowData>() {
                        @Override
                        public void close() throws Exception {
                            closedFuture.complete(null);
                        }

                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public RowData next() {
                            throw new UnsupportedOperationException();
                        }
                    },
                    false,
                    ResultKind.SUCCESS_WITH_CONTENT,
                    JobID.generate());
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
