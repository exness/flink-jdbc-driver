package org.apache.flink.table.jdbc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for Flink JDBC driver. */
@Internal
public class FlinkDriverOptions {
    public static final ConfigOption<Long> STREAMING_RESULT_HEARTBEAT_INTERVAL_MS =
            ConfigOptions.key("jdbc.streaming.result.heartbeat.interval.ms")
                    .longType()
                    .defaultValue(9000L)
                    .withDescription(
                            "If enabled, a heartbeat record will be periodically added to the result set when the underlying streaming job does not produce new data. Set to -1 or 0 to disable.");

    public static final ConfigOption<ResultMode> RESULT_MODE =
            ConfigOptions.key("jdbc.output.result-mode")
                    .enumType(ResultMode.class)
                    .defaultValue(ResultMode.TABLE)
                    .withDescription(
                            "Sets the result mode. TABLE means the driver will show only "
                                    + "the final state (records with RowKind=INSERT, UPDATE_AFTER). "
                                    + "CHANGELOG means the driver will show all the records. If CHANGELOG "
                                    + "is enabled, the driver will add the column `row_kind` to the ResultSet.");

    private FlinkDriverOptions() {}
}
