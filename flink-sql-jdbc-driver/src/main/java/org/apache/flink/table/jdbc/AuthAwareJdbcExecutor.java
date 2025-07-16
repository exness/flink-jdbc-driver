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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.jdbc.utils.AuthUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

import static org.apache.flink.table.jdbc.utils.AuthUtils.fetchAccessToken;
import static org.apache.flink.table.jdbc.utils.AuthUtils.openLoginPage;

class AuthAwareJdbcExecutor extends JdbcExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(AuthAwareJdbcExecutor.class);
    private static final long HEARTBEAT_INTERVAL_MILLISECONDS = 60_000L;
    private static final int ACCESS_TOKEN_MAX_RETRY_ATTEMPTS = 3;
    private static final long ACCESS_TOKEN_INITIAL_RETRY_DELAY_MILLIS = 100L;
    private static final String AUTH_TOKEN_ERROR_MSG =
            "Error: `jdbc.auth.token` is not set, invalid or has expired. Please login and set it in the driver properties.";
    private final String accessTokenEndpoint;
    private final String loginEndpoint;
    private final String refreshToken;
    private String accessToken;

    public AuthAwareJdbcExecutor(
            DefaultContext defaultContext,
            InetSocketAddress gatewayAddress,
            String sessionId,
            RowFormat rowFormat,
            String refreshToken,
            String accessTokenEndpoint,
            String loginEndpoint) {
        this(
                defaultContext,
                socketToUrl(gatewayAddress),
                sessionId,
                HEARTBEAT_INTERVAL_MILLISECONDS,
                rowFormat,
                refreshToken,
                accessTokenEndpoint,
                loginEndpoint);
    }

    @VisibleForTesting
    AuthAwareJdbcExecutor(
            DefaultContext defaultContext,
            InetSocketAddress gatewayAddress,
            String sessionId,
            long heartbeatInterval,
            String refreshToken,
            String accessTokenEndpoint,
            String loginEndpoint) {
        this(
                defaultContext,
                socketToUrl(gatewayAddress),
                sessionId,
                heartbeatInterval,
                RowFormat.PLAIN_TEXT,
                refreshToken,
                accessTokenEndpoint,
                loginEndpoint);
    }

    @VisibleForTesting
    AuthAwareJdbcExecutor(
            DefaultContext defaultContext,
            URL gatewayUrl,
            String sessionId,
            long heartbeatInterval,
            RowFormat rowFormat,
            String refreshToken,
            String accessTokenEndpoint,
            String loginEndpoint) {
        super(defaultContext, gatewayUrl, sessionId, heartbeatInterval, rowFormat);
        this.accessTokenEndpoint = accessTokenEndpoint;
        this.loginEndpoint = loginEndpoint;
        this.refreshToken = refreshToken;
        this.accessToken = null;
    }

    private void refreshAccessToken() {
        long delay = ACCESS_TOKEN_INITIAL_RETRY_DELAY_MILLIS;
        for (int i = 1; i <= ACCESS_TOKEN_MAX_RETRY_ATTEMPTS; i++) {
            try {
                accessToken = fetchAccessToken(accessTokenEndpoint, refreshToken);
                this.getCustomHttpHeaders()
                        .removeIf(header -> "Authorization".equalsIgnoreCase(header.getName()));
                this.getCustomHttpHeaders()
                        .add(new HttpHeader("Authorization", "Bearer " + accessToken));
                LOG.info("Added Authorization header with access token.");
                return;
            } catch (AuthUtils.NotAuthorizedException e) {
                System.out.println("qqq Not authorized exception");
                openLoginPage(loginEndpoint);
                throw new SqlExecutionException(AUTH_TOKEN_ERROR_MSG);
            } catch (IOException e) {
                if (i == ACCESS_TOKEN_MAX_RETRY_ATTEMPTS) {
                    throw new SqlExecutionException("Cannot set auth header: " + e.getCause());
                }
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SqlExecutionException(
                            "Retry interrupted while refreshing access token", ie);
                }
                delay *= 2;
            }
        }
    }

    @Override
    public StatementResult executeStatement(String statement) {
        System.out.println("qqq Executing statement: " + statement);
        try {
            return super.executeStatement(statement);
        } catch (SqlExecutionException e) {
            if (e.getCause() != null) {
                final String cause = e.getCause().getMessage().toLowerCase();
                if (cause.contains("jdbc.auth.token") || cause.contains("invalid access token")) {
                    refreshAccessToken();
                    return super.executeStatement(statement);
                }
            }
            throw e;
        }
    }
}
