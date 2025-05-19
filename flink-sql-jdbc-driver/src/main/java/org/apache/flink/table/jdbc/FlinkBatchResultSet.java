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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.jdbc.utils.StatementResultIterator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.sql.SQLException;
import java.sql.Statement;

/** ResultSet for flink jdbc driver for batch jobs. */
public class FlinkBatchResultSet extends FlinkResultSet {
    private final CloseableIterator<RowData> iterator;

    public FlinkBatchResultSet(
            Statement statement,
            CloseableIterator<RowData> iterator,
            ResolvedSchema schema,
            boolean asChangeLog) {
        super(statement, schema, asChangeLog, false);
        this.iterator = iterator;
    }

    public FlinkBatchResultSet(Statement statement, StatementResult result, boolean asChangeLog) {
        this(statement, new StatementResultIterator(result), result.getResultSchema(), asChangeLog);
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            iterator.close();
        } catch (Exception e) {
            throw new SQLException("Close result iterator fail", e);
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        while (iterator.hasNext()) {
            currentRow = iterator.next();
            if (currentRow == null) {
                continue;
            }
            RowKind rowKind = currentRow.getRowKind();
            if (!asChangeLog && (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE)) {
                continue;
            }
            currentRow = enrich(currentRow, false);
            return true;
        }
        return false;
    }
}
