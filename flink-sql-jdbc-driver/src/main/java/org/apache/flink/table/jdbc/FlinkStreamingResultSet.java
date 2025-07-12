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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Collections;
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

    private Object getDefaultValueForType(LogicalType dataType) throws SQLException {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.FALSE;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return 0;
            case BIGINT:
                return 0L;
            case FLOAT:
                return 0.0f;
            case DOUBLE:
                return 0.0;
            case DECIMAL:
                return DecimalData.zero(1, 1);
            case BINARY:
            case VARBINARY:
            case RAW:
                return new byte[0];
            case VARCHAR:
            case CHAR:
                return StringData.fromString("");
            case ARRAY:
                return new GenericArrayData(new Object[0]);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromEpochMillis(0);
            case ROW:
                RowType rowType = (RowType) dataType;
                GenericRowData result = new GenericRowData(rowType.getFieldCount());
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    LogicalType nestedType = rowType.getTypeAt(i);
                    if (!nestedType.isNullable()) {
                        result.setField(i, getDefaultValueForType(nestedType));
                    } else {
                        result.setField(i, null);
                    }
                }
                return result;
            case MAP:
                return new GenericMapData(Collections.emptyMap());
            case MULTISET: // TODO
            default:
                throw new SQLException("Unsupported data type: " + dataType);
        }
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
            // create a heartbeat record -- a record with only the `streaming_state` column filled
            GenericRowData genericRow = new GenericRowData(dataTypeList.size());
            for (int i = 0; i < dataTypeList.size(); ++i) {
                LogicalType type = dataTypeList.get(i).getLogicalType();
                if (!type.isNullable()) {
                    genericRow.setField(i, getDefaultValueForType(type));
                }
            }
            currentRow = enrich(genericRow, true);
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
