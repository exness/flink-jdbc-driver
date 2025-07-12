package org.apache.flink.table.jdbc.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StackTraceParser {
    private static final Pattern causedByPattern =
            Pattern.compile("^Caused by:\\s*(\\S+Exception)(?:\\s*:\\s*(.*))?$");

    private StackTraceParser() {}

    public static String extractRootCause(String stackTrace) {
        if (stackTrace == null || stackTrace.isEmpty()) {
            return null;
        }

        String[] lines = stackTrace.split("\\r?\\n");

        int endIndex = lines.length;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].contains("End of exception on server side>")) {
                endIndex = i;
                break;
            }
        }

        String lastExceptionMessage = null;

        for (int i = 0; i < endIndex; i++) {
            final String line = lines[i].trim();
            Matcher matcher = causedByPattern.matcher(line);
            if (matcher.matches()) {
                final String exceptionClass = matcher.group(1).trim();
                final String exceptionMessage =
                        matcher.group(2) != null ? matcher.group(2).trim() : "";

                if (exceptionMessage.isEmpty()) {
                    continue;
                }

                final StringBuilder fullExceptionMessage = new StringBuilder(exceptionMessage);

                int j = i + 1;
                while (j < endIndex) {
                    final String l = lines[j].trim();
                    if (l.startsWith("Caused by:") || l.startsWith("at")) {
                        break;
                    }
                    fullExceptionMessage.append("\n").append(l);
                    j++;
                }

                // CalciteContextException provides a better error description than the one deeper
                // in the stack trace, so return it if found
                if (exceptionClass.equals("org.apache.calcite.runtime.CalciteContextException")) {
                    return fullExceptionMessage.toString();
                }
                lastExceptionMessage = fullExceptionMessage.toString();
            }
        }

        return lastExceptionMessage;
    }
}
