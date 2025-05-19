# flink-exness-jdbc-driver
The Flink JDBC Driver is a Java library for enabling clients to send Flink SQL to your Flink cluster via the SQL Gateway. Full documentation is available on the Flink [website](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/jdbcdriver/).

# Exness contributions:

- added support for missing types (rows, arrays, etc.)
- added proper errors parsing
- added support for streaming results
- made it work with DataGrip
- todo

# Driver Options
These options can be passed through the JDBC connection URL using standard key-value properties, or enabled / disabled by executing `set OPTION_NAME = OPTION_VALUE;` 

| Option name                                   | Type     | Default | Values                                   | Description |
|-----------------------------------------------|----------|---------|------------------------------------------|-------------|
| `jdbc.streaming.result.heartbeat.interval.ms` | `long`   | `9000`  | Any non-negative number; `0` or `-1` to disable | Adds heartbeat rows to prevent idle timeout when streaming job is silent. |
| `jdbc.output.result-mode`                     | `enum`   | `TABLE` | `TABLE`, `CHANGELOG`                     | Determines whether the result set shows only the final state (`TABLE`) or full change stream (`CHANGELOG`). Adds `row_kind` column in `CHANGELOG` mode. |

# Build
``
mvn clean package -pl flink-sql-jdbc-driver-bundle
``