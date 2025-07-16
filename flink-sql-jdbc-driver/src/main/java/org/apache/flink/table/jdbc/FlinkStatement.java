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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.jdbc.utils.StackTraceParser;

import javax.annotation.concurrent.NotThreadSafe;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.jdbc.FlinkDriverOptions.RESULT_MODE;
import static org.apache.flink.table.jdbc.FlinkDriverOptions.STREAMING_RESULT_HEARTBEAT_INTERVAL_MS;

/** Statement for flink jdbc driver. Notice that the statement is not thread safe. */
@NotThreadSafe
public class FlinkStatement extends BaseStatement {
    private final FlinkConnection connection;
    private final Executor executor;
    private FlinkResultSet currentResults;
    private boolean hasResults;
    private volatile boolean closed;
    private int updateCount = 0;
    private int fetchSize = 5000;

    public FlinkStatement(FlinkConnection connection) {
        this.connection = connection;
        this.executor = connection.getExecutor();
        this.currentResults = null;
    }

    /**
     * Execute a SELECT query.
     *
     * @param sql an SQL statement to be sent to the database, typically a static SQL <code>SELECT
     *     </code> statement
     * @return the select query result set.
     * @throws SQLException the thrown exception
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        StatementResult result = executeInternal(sql);
        if (!result.isQueryResult()) {
            result.close();
            throw new SQLException(String.format("Statement[%s] is not a query.", sql));
        }
        setCurrentResults(result);
        return currentResults;
    }

    private boolean isStreamingMode() {
        return executor.getSessionConfig().get(RUNTIME_MODE).equals(RuntimeExecutionMode.STREAMING);
    }

    private void setCurrentResults(StatementResult result) {
        final boolean asChangeLog =
                executor.getSessionConfig().get(RESULT_MODE) == ResultMode.CHANGELOG;

        if (isStreamingMode()) {
            currentResults =
                    new FlinkStreamingResultSet(
                            this,
                            result,
                            asChangeLog,
                            executor.getSessionConfig()
                                    .get(STREAMING_RESULT_HEARTBEAT_INTERVAL_MS));
        } else {
            currentResults = new FlinkBatchResultSet(this, result, asChangeLog);
        }
        hasResults = true;
    }

    private void clearCurrentResults() throws SQLException {
        if (currentResults == null) {
            return;
        }
        currentResults.close();
        currentResults = null;
        hasResults = false;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        cancel();
        connection.removeStatement(this);
        closed = true;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        clearCurrentResults();
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public void clearWarnings() throws SQLException {}

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("This result set is already closed");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException("Fetch size must be positive");
        }
        fetchSize = rows;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        // ignore maxRows
    }

    /**
     * Execute a sql statement. Notice that the <code>INSERT</code> statement in Flink would return
     * job id as result set.
     *
     * @param sql any SQL statement
     * @return True if there is result set for the statement.
     * @throws SQLException the thrown exception.
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        StatementResult result = executeInternal(sql);
        if (result.isQueryResult() || result.getResultKind() == ResultKind.SUCCESS_WITH_CONTENT) {
            setCurrentResults(result);
            return true;
        }

        hasResults = false;
        return false;
    }

    private StatementResult executeInternal(String sql) throws SQLException {
        checkClosed();
        clearCurrentResults();
        try {
            return executor.executeStatement(sql);
        } catch (SqlExecutionException e) {
            final String rootCause =
                    StackTraceParser.extractRootCause(
                            e.getCause() == null ? null : e.getCause().getMessage());
            if (rootCause != null) {
                throw new SQLException(rootCause, e.getCause());
            }
            throw new SQLException(
                    e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();

        if (currentResults == null) {
            throw new SQLException("No result set in the current statement.");
        }
        if (currentResults.isClosed()) {
            throw new SQLException("Result set has been closed");
        }
        return currentResults;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        int toReturn = getUpdateCountInternal();
        updateCount = -1;
        return toReturn;
    }

    public int getUpdateCountInternal() throws SQLException {
        if (hasResults) {
            return -1;
        } else {
            return updateCount;
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
}
