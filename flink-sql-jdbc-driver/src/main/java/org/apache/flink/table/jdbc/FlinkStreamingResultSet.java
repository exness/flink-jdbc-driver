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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.result.ChangelogCollectResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** ResultSet for flink jdbc driver which supports streaming mode. */
public class FlinkStreamingResultSet extends FlinkResultSet {
    private final ChangelogCollectResult changelog;
    private final long heartBeatIntervalMs;
    Queue<RowData> recordsQueue = new ArrayDeque<>();
    private boolean isStreamFinished = false;
    private Long idleStartTimeMs = null;

    public FlinkStreamingResultSet(
            Statement statement,
            StatementResult result,
            boolean asChangeLog,
            long heartBeatIntervalMs) {
        this(
                statement,
                new ChangelogCollectResult(result),
                result.getResultSchema(),
                asChangeLog,
                heartBeatIntervalMs);
    }

    public FlinkStreamingResultSet(
            Statement statement,
            ChangelogCollectResult changelog,
            ResolvedSchema schema,
            boolean asChangeLog,
            long heartBeatIntervalMs) {
        super(statement, schema, asChangeLog, heartBeatIntervalMs > 0);
        this.changelog = changelog;
        this.heartBeatIntervalMs = heartBeatIntervalMs;
    }

    public boolean isStreamFinished() {
        return isStreamFinished;
    }

    private boolean tryIterate() throws SQLException {
        checkClosed();
        if (isStreamFinished()) {
            return false;
        }
        if (recordsQueue.isEmpty()) {
            final TypedResult<List<RowData>> changes = changelog.retrieveChanges();
            if (changes.getType() == TypedResult.ResultType.EOS) {
                isStreamFinished = true;
                return false;
            }
            if (changes.getType() == TypedResult.ResultType.PAYLOAD) {
                recordsQueue.addAll(changes.getPayload());
            }
        }

        while (!recordsQueue.isEmpty()) {
            currentRow = recordsQueue.poll();
            if (currentRow == null) {
                continue;
            }
            RowKind rowKind = currentRow.getRowKind();
            if (!asChangeLog && (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE)) {
                continue;
            }
            currentRow = enrich(currentRow, false);
            idleStartTimeMs = null;
            return true;
        }

        if (idleStartTimeMs == null) {
            idleStartTimeMs = System.currentTimeMillis();
        } else if (heartBeatIntervalMs > 0
                && System.currentTimeMillis() - idleStartTimeMs > heartBeatIntervalMs) {
            // create a heartbeat record -- a record with only `streaming_state` column filled
            currentRow = enrich(new GenericRowData(RowKind.INSERT, dataTypeList.size()), true);
            idleStartTimeMs = null;
            return true;
        }

        return false;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (isStreamFinished()) {
            return false;
        }
        while (!tryIterate()) {
            checkClosed();
            if (Thread.currentThread().isInterrupted() || isStreamFinished()) {
                return false;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            changelog.close();
            closed = true;
        } catch (Exception e) {
            throw new SQLException("Close result iterator fail", e);
        }
    }
}
