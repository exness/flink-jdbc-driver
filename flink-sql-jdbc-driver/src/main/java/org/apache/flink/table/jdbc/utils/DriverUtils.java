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

import javax.annotation.Nullable;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class DriverUtils {
    public static final String STREAM_STATE_ACTIVE_VALUE = "Active";
    private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();
    private static final String ROW_KIND_COLUMN_PREFIX = "row_kind$";
    private static final String STREAM_STATE_COLUMN_PREFIX = "stream_state$";
    private static final int ROW_KIND_COLUMN_SUFFIX_LENGTH = 4;
    private static final int STREAM_STATE_COLUMN_SUFFIX_LENGTH = 4;

    public static String getHeartBeatMessageIdle() {
        return String.format(
                String.format(
                        "No new data @ %s",
                        new SimpleDateFormat("HH:mm:ss z")
                                .format(new Date(System.currentTimeMillis()))));
    }

    public static String getHeartBeatMessageActive() {
        return STREAM_STATE_ACTIVE_VALUE;
    }

    /**
     * Ensures that the given object reference is not null. Upon violation, a {@code
     * NullPointerException} with the given message is thrown.
     *
     * @param reference The object reference
     * @param errorMessage The message for the {@code NullPointerException} that is thrown if the
     *     check fails.
     * @return The object reference itself (generically typed).
     * @throws NullPointerException Thrown, if the passed reference was null.
     */
    public static <T> T checkNotNull(@Nullable T reference, @Nullable String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * Checks the given boolean condition, and throws an {@code IllegalArgumentException} if the
     * condition is not met (evaluates to {@code false}). The exception will have the given error
     * message.
     *
     * @param condition The condition to check
     * @param errorMessage The message for the {@code IllegalArgumentException} that is thrown if
     *     the check fails.
     * @throws IllegalArgumentException Thrown, if the condition is violated.
     */
    public static void checkArgument(boolean condition, @Nullable Object errorMessage) {
        if (!condition) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    /**
     * Checks if the string is null, empty, or contains only whitespace characters. A whitespace
     * character is defined via {@link Character#isWhitespace(char)}.
     *
     * @param str The string to check
     * @return True, if the string is null or blank, false otherwise.
     */
    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }

        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Generate map from given properties.
     *
     * @param properties the given properties
     * @return the result map
     */
    public static Map<String, String> fromProperties(Properties properties) {
        Map<String, String> map = new HashMap<>();
        Enumeration<?> e = properties.propertyNames();

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            map.put(key, properties.getProperty(key));
        }

        return map;
    }

    public static String generateRowKindColumnName() {
        final StringBuilder result = new StringBuilder(ROW_KIND_COLUMN_PREFIX);
        for (int i = 0; i < ROW_KIND_COLUMN_SUFFIX_LENGTH; i++) {
            int index = RANDOM.nextInt(CHARACTERS.length());
            result.append(CHARACTERS.charAt(index));
        }
        return result.toString();
    }

    public static String generateStreamStateColumnName() {
        final StringBuilder result = new StringBuilder(STREAM_STATE_COLUMN_PREFIX);
        for (int i = 0; i < STREAM_STATE_COLUMN_SUFFIX_LENGTH; i++) {
            int index = RANDOM.nextInt(CHARACTERS.length());
            result.append(CHARACTERS.charAt(index));
        }
        return result.toString();
    }

    public static String jdbcToHttpUrl(String driverUri, String suffix) {
        if (!driverUri.startsWith("jdbc:flink://")) {
            throw new IllegalArgumentException("Invalid driverUri: " + driverUri);
        }
        String address = driverUri.substring("jdbc:flink://".length());
        // todo: change to https
        return "http://" + address + (suffix.startsWith("/") ? suffix : "/" + suffix);
    }
}
