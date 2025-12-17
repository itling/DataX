package com.alibaba.datax.plugin.tdengine30reader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.tdengine30writer.Key;
import com.alibaba.datax.common.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TDengineReader is a DataX reader plugin that reads data from TDengine database.
 * It supports reading from regular tables and super tables, with options for time range splitting
 * and subtable batching for optimized performance.
 */
public class TDengineReader extends Reader {

    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Job class handles the configuration initialization, task splitting, and resource cleanup for TDengineReader.
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;

        /**
         * Initialize the job configuration, validate parameters, and set default values if not provided.
         */
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // check username
            String username = this.originalConfig.getString(Key.USERNAME);
            if (StringUtils.isBlank(username))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.USERNAME + "] is not set.");

            // check password
            String password = this.originalConfig.getString(Key.PASSWORD);
            if (StringUtils.isBlank(password))
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.PASSWORD + "] is not set.");

            // check connection
            List<Configuration> connectionList = this.originalConfig.getListConfiguration(Key.CONNECTION);
            if (connectionList == null || connectionList.isEmpty())
                throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.CONNECTION + "] is not set.");
            for (int i = 0; i < connectionList.size(); i++) {
                Configuration conn = connectionList.get(i);
                // check jdbcUrl
                List<Object> jdbcUrlList = conn.getList(Key.JDBC_URL);
                if (jdbcUrlList == null || jdbcUrlList.isEmpty()) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.JDBC_URL + "] of connection[" + (i + 1) + "] is not set.");
                }
                // check table/querySql
                List<Object> querySqlList = conn.getList(Key.QUERY_SQL);
                if (querySqlList == null || querySqlList.isEmpty()) {
                    String querySql = conn.getString(Key.QUERY_SQL);
                    if (StringUtils.isBlank(querySql)) {
                        List<Object> table = conn.getList(Key.TABLE);
                        if (table == null || table.isEmpty())
                            throw DataXException.asDataXException(TDengineReaderErrorCode.REQUIRED_VALUE,
                                    "The parameter [" + Key.TABLE + "] of connection[" + (i + 1) + "] is not set.");
                    }
                }
            }

            SimpleDateFormat format = new SimpleDateFormat(DATETIME_FORMAT);
            // check beginDateTime
            String beginDatetime = this.originalConfig.getString(Key.BEGIN_DATETIME);
            long start = Long.MIN_VALUE;
            if (!StringUtils.isBlank(beginDatetime)) {
                try {
                    start = format.parse(beginDatetime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.BEGIN_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
                }
            }
            // check endDateTime
            String endDatetime = this.originalConfig.getString(Key.END_DATETIME);
            long end = Long.MAX_VALUE;
            if (!StringUtils.isBlank(endDatetime)) {
                try {
                    end = format.parse(endDatetime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.END_DATETIME + "] needs to conform to the [" + DATETIME_FORMAT + "] format.");
                }
            }
            if (start >= end)
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.BEGIN_DATETIME + "] should be less than the parameter [" + Key.END_DATETIME + "].");

        }

        /**
         * Clean up resources after the job is completed.
         */
        @Override
        public void destroy() {

        }

        /**
         * Split the job into multiple tasks based on the configuration.
         * Each JDBC URL in the connection list becomes a separate task.
         * 
         * @param adviceNumber The advised number of tasks to split into (not used in this implementation)
         * @return List of configurations for each split task
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();

            List<Configuration> connectionList = this.originalConfig.getListConfiguration(Key.CONNECTION);
            for (Configuration conn : connectionList) {
                List<String> jdbcUrlList = conn.getList(Key.JDBC_URL, String.class);
                for (String jdbcUrl : jdbcUrlList) {
                    Configuration clone = this.originalConfig.clone();
                    clone.set(Key.JDBC_URL, jdbcUrl);
                    clone.set(Key.TABLE, conn.getList(Key.TABLE));
                    clone.set(Key.QUERY_SQL, conn.getList(Key.QUERY_SQL));
                    clone.remove(Key.CONNECTION);
                    configurations.add(clone);
                }
            }

            LOG.info("Configuration: {}", configurations);
            return configurations;
        }
    }

    /**
     * Task class handles the actual data reading operation from TDengine database.
     * It manages database connections, SQL execution, and data record construction.
     */
    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private String mandatoryEncoding;
        private Connection conn;

        private List<String> tables;
        private List<String> columns;
        private String startTime;
        private String endTime;
        private String where;
        private List<String> querySql;
        private String splitInterval;
        private boolean reverseTime;
        private int splitSubtable;
        private int retryTimes;
        private int retryInterval;
        private boolean exponentialRetry;
        private List<Class<?>> retryExceptionClasses;

        /**
         * Static initializer to load TDengine JDBC drivers.
         */
        static {
            try {
                Class.forName("com.taosdata.jdbc.TSDBDriver");
                Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            } catch (ClassNotFoundException ignored) {
                LOG.warn(ignored.getMessage(), ignored);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();

            String user = readerSliceConfig.getString(Key.USERNAME);
            String password = readerSliceConfig.getString(Key.PASSWORD);

            String url = readerSliceConfig.getString(Key.JDBC_URL);
            try {
                this.conn = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.CONNECTION_FAILED,
                        "The parameter [" + Key.JDBC_URL + "] : " + url + " failed to connect since: " + e.getMessage(), e);
            }

            this.tables = readerSliceConfig.getList(Key.TABLE, String.class);
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);
            this.startTime = readerSliceConfig.getString(Key.BEGIN_DATETIME);
            this.endTime = readerSliceConfig.getString(Key.END_DATETIME);
            this.where = readerSliceConfig.getString(Key.WHERE, "_c0 > " + Long.MIN_VALUE);
            this.querySql = readerSliceConfig.getList(Key.QUERY_SQL, String.class);
            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "UTF-8");
            this.splitInterval = readerSliceConfig.getString(Key.SPLIT_INTERVAL);
            this.reverseTime = readerSliceConfig.getBool(Key.REVERSE_TIME, false);
            this.splitSubtable = readerSliceConfig.getInt(Key.SPLIT_SUBTABLE, 0);
            this.retryTimes = readerSliceConfig.getInt(Key.RETRY_TIMES,3);
            this.retryInterval = readerSliceConfig.getInt(Key.RETRY_INTERVAL,1000);
            this.exponentialRetry = readerSliceConfig.getBool(Key.EXPONENTIAL_RETRY, false);
            // Initialize retry exception class list
            List<String> defaultRetryExceptions = Arrays.asList(
                    "java.sql.SQLException",
                    "java.net.ConnectException",
                    "com.taosdata.jdbc.TSDBDriverException"
            );
            this.retryExceptionClasses = loadRetryExceptionClasses(
                    readerSliceConfig.getList(Key.RETRY_EXCEPTION_CLASSES, defaultRetryExceptions, String.class));
        }

        /**
         * Clean up resources after the task is completed, primarily closing the database connection.
         */
        @Override
        public void destroy() {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        
        /**
         * Load configured retry exception classes (convert class name strings to Class objects)
         * 
         * @param exceptionClassNames List of exception class names (e.g., java.sql.SQLException)
         * @return List of exception Class objects
         */
        private List<Class<?>> loadRetryExceptionClasses(List<String> exceptionClassNames) {
            List<Class<?>> exceptionClasses = new ArrayList<>();
            if (exceptionClassNames == null || exceptionClassNames.isEmpty()) {
                return exceptionClasses;
            }

            for (String className : exceptionClassNames) {
                try {
                    // Load exception class
                    Class<?> clazz = Class.forName(className);
                    // Check if it's a subclass of Exception
                    if (Exception.class.isAssignableFrom(clazz)) {
                        exceptionClasses.add(clazz);
                        LOG.info("Successfully loaded retry exception class: {}", className);
                    } else {
                        LOG.warn("The configured class {} is not a subclass of Exception, skipping loading", className);
                    }
                } catch (ClassNotFoundException e) {
                    LOG.error("Failed to load retry exception class {}, skipping this class", className, e);
                }
            }
            return exceptionClasses;
        }

        /**
         * Parse the split interval string into milliseconds.
         * Supported units: d(day), h(hour), m(minute), s(second)
         * 
         * @param interval The split interval string (e.g., "1h" for 1 hour)
         * @return The interval in milliseconds
         */
        private long parseSplitInterval(String interval) {
            if (StringUtils.isBlank(interval)) {
                return 0;
            }
            interval = interval.trim();
            char unit = interval.charAt(interval.length() - 1);
            long value = Long.parseLong(interval.substring(0, interval.length() - 1));
            switch (unit) {
                case 'd':
                    return value * 24 * 60 * 60 * 1000;
                case 'h':
                    return value * 60 * 60 * 1000;
                case 'm':
                    return value * 60 * 1000;
                case 's':
                    return value * 1000;
                default:
                    throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                            "Invalid splitInterval unit: " + unit + ". Supported units: d(day), h(hour), m(minute), s(second)");
            }
        }

        /**
         * Get all subtable names from the super table
         * 
         * @param superTable Super table name
         * @return List of subtable names
         */
        private List<String> getSubtableNames(String superTable) {
            try {
                return RetryUtil.executeWithRetry(() -> {
                    List<String> subtableNames = new ArrayList<>();
                    String sql = "select distinct tbname from " + superTable;
                    try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) {
                            subtableNames.add(rs.getString(1));
                        }
                    } catch (SQLException e) {
                        throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                                "Failed to get subtable names from super table: " + superTable + " since: " + e.getMessage(), e);
                    }
                    return subtableNames;
                }, retryTimes, retryInterval, exponentialRetry, retryExceptionClasses);
            } catch (Exception e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.READER_SQL_EXECUTION_FAILED,
                        "Failed to get subtable names, all retry attempts exhausted", e);
            }
        }

        /**
         * Split a datetime range into multiple smaller ranges based on the specified interval.
         * 
         * @param beginTime The start time of the range
         * @param endTime The end time of the range
         * @param splitInterval The interval to split the range by
         * @param reverse Whether to split in reverse order (from endTime to beginTime)
         * @return List of time ranges in the format "startTime,endTime"
         */
        private List<String> splitDateTimeRange(String beginTime, String endTime, String splitInterval, boolean reverse) {
            List<String> timeRanges = new ArrayList<>();
            if (StringUtils.isBlank(beginTime) || StringUtils.isBlank(endTime) || StringUtils.isBlank(splitInterval)) {
                timeRanges.add(beginTime + "," + endTime);
                return timeRanges;
            }

            SimpleDateFormat format = new SimpleDateFormat(DATETIME_FORMAT);
            try {
                java.util.Date startDate =  format.parse(beginTime);
                java.util.Date endDate =  format.parse(endTime);
                long intervalMs = parseSplitInterval(splitInterval);

                if (intervalMs <= 0) {
                    timeRanges.add(beginTime + "," + endTime);
                    return timeRanges;
                }

                if (reverse) {
                    // Reverse time query: split from endTime to beginTime
                    long currentTime = endDate.getTime();
                    long startMs = startDate.getTime();

                    while (currentTime > startMs) {
                        long prevTime = Math.max(currentTime - intervalMs, startMs);
                        timeRanges.add(format.format(new Date(prevTime)) + "," + format.format(new Date(currentTime)));
                        currentTime = prevTime;
                    }
                } else {
                    // Normal time query: split from startTime to endTime
                    long currentTime = startDate.getTime();
                    long endMs = endDate.getTime();

                    while (currentTime < endMs) {
                        long nextTime = Math.min(currentTime + intervalMs, endMs);
                        timeRanges.add(format.format(new Date(currentTime)) + "," + format.format(new Date(nextTime)));
                        currentTime = nextTime;
                    }
                }
            } catch (ParseException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE,
                        "Invalid datetime format: " + e.getMessage(), e);
            }

            return timeRanges;
        }

        /**
         * Start reading data from TDengine database and send records to the writer.
         * This method handles SQL generation, execution, and record construction.
         * 
         * @param recordSender The RecordSender to send the constructed records
         */
        @Override
        public void startRead(RecordSender recordSender) {
            List<String> sqlList = new ArrayList<>();

            if (querySql == null || querySql.isEmpty()) {
                for (String table : tables) {
                    // If splitSubtable is set, get subtable names and generate SQL in batches
                    if (splitSubtable > 0) {
                        List<String> subtableNames = getSubtableNames(table);
                        // Batch subtable names, with each batch size being splitSubtable
                        LOG.info("splitSubtable is set to {}, will split {} subtable(s) into {} batch(es).", splitSubtable, subtableNames.size(), (subtableNames.size() + splitSubtable - 1) / splitSubtable);
                        for (int i = 0; i < subtableNames.size(); i += splitSubtable) {
                            int end = Math.min(i + splitSubtable, subtableNames.size());
                            List<String> batchSubtables = subtableNames.subList(i, end);
                            
                            // Generate subtable IN condition
                            StringBuilder inClause = new StringBuilder("tbname in ('");
                            inClause.append(StringUtils.join(batchSubtables, "','"));
                            inClause.append("')");
                            
                            // If splitInterval is set, split the time range
                            if (!StringUtils.isBlank(splitInterval) && !StringUtils.isBlank(startTime) && !StringUtils.isBlank(endTime)) {
                                LOG.info("splitInterval is set to {}, will split time range {} - {} into batches. tbname count: {}", splitInterval, startTime, endTime, batchSubtables.size());
                                List<String> timeRanges = splitDateTimeRange(startTime, endTime, splitInterval, reverseTime);
                                LOG.info("splitDateTimeRange result: {}", timeRanges);
                                for (String timeRange : timeRanges) {
                                    String[] times = timeRange.split(",");
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("select ").append(StringUtils.join(columns, ",")).append(" from ").append(table).append(" ");
                                    sb.append("where ").append(where);
                                    sb.append(" and ").append(inClause);
                                    sb.append(" and _c0 >= '").append(times[0]).append("'");
                                    sb.append(" and _c0 < '").append(times[1]).append("'");
                                    if (reverseTime) {
                                        sb.append(" order by _c0 desc");
                                    }
                                    String sql = sb.toString().trim();
                                    sqlList.add(sql);
                                }
                            } else {
                                LOG.info("splitInterval is not set, will query table {} directly with time range {} - {}. tbname count: {}", table, startTime, endTime, batchSubtables.size());
                                // Don't split the time range
                                StringBuilder sb = new StringBuilder();
                                sb.append("select ").append(StringUtils.join(columns, ",")).append(" from ").append(table).append(" ");
                                sb.append("where ").append(where);
                                sb.append(" and ").append(inClause);
                                if (!StringUtils.isBlank(startTime)) {
                                    sb.append(" and _c0 >= '").append(startTime).append("'");
                                }
                                if (!StringUtils.isBlank(endTime)) {
                                    sb.append(" and _c0 < '").append(endTime).append("'");
                                }
                                if (reverseTime) {
                                    sb.append(" order by _c0 desc");
                                }
                                String sql = sb.toString().trim();
                                sqlList.add(sql);
                            }
                        }
                    } else {
                        LOG.info("splitSubtable is not set, will query table {} directly.", table);
                        // Don't use subtable batch query
                        // If splitInterval is set, split the time range
                        if (!StringUtils.isBlank(splitInterval) && !StringUtils.isBlank(startTime) && !StringUtils.isBlank(endTime)) {
                            LOG.info("splitInterval is set to {}, will split time range {} - {} into batches.", splitInterval, startTime, endTime);
                            List<String> timeRanges = splitDateTimeRange(startTime, endTime, splitInterval, reverseTime);
                            LOG.info("splitDateTimeRange result: {}", timeRanges);
                            for (String timeRange : timeRanges) {
                                String[] times = timeRange.split(",");
                                StringBuilder sb = new StringBuilder();
                                sb.append("select ").append(StringUtils.join(columns, ",")).append(" from ").append(table).append(" ");
                                sb.append("where ").append(where);
                                sb.append(" and _c0 >= '").append(times[0]).append("'");
                                sb.append(" and _c0 < '").append(times[1]).append("'");
                                if (reverseTime) {
                                    sb.append(" order by _c0 desc");
                                }
                                String sql = sb.toString().trim();
                                sqlList.add(sql);
                            }
                        } else {
                            LOG.info("splitInterval is not set, will query table {} directly with time range {} - {}.", table, startTime, endTime);
                            // Don't split, use original time range
                            StringBuilder sb = new StringBuilder();
                            sb.append("select ").append(StringUtils.join(columns, ",")).append(" from ").append(table).append(" ");
                            sb.append("where ").append(where);
                            if (!StringUtils.isBlank(startTime)) {
                                sb.append(" and _c0 >= '").append(startTime).append("'");
                            }
                            if (!StringUtils.isBlank(endTime)) {
                                sb.append(" and _c0 < '").append(endTime).append("'");
                            }
                            if (reverseTime) {
                                sb.append(" order by _c0 desc");
                            }
                            String sql = sb.toString().trim();
                            sqlList.add(sql);
                        }
                    }
                }
            } else {
                sqlList.addAll(querySql);
            }

            long startTime = System.currentTimeMillis();
            int currentIndex = 0;
            int totalSqlCount = sqlList.size();
            
            LOG.info("Total SQL to execute: {}", totalSqlCount);
            
            for (String sql : sqlList) {
                long sqlStartTime = System.currentTimeMillis();
                try {
                    RetryUtil.executeWithRetry(() -> {
                        try (Statement stmt = conn.createStatement()) {
                            ResultSet rs = stmt.executeQuery(sql);
                            while (rs.next()) {
                                Record record = buildRecord(recordSender, rs, mandatoryEncoding);
                                recordSender.sendToWriter(record);
                            }
                        } catch (SQLException e) {
                            LOG.error(e.getMessage(), e);
                            throw e; 
                        }
                        return null; 
                    }, retryTimes, retryInterval, exponentialRetry, retryExceptionClasses);
                } catch (Exception e) {
                    throw DataXException.asDataXException(TDengineReaderErrorCode.READER_SQL_EXECUTION_FAILED,
                            "Failed to execute SQL, all retry attempts exhausted: " + sql, e);
                }
                
                currentIndex++;
                long currentTime = System.currentTimeMillis();
                
                // Print progress for each SQL execution
                long elapsedSeconds = (currentTime - sqlStartTime) / 1000;
                long totalElapsedSeconds = (currentTime - startTime) / 1000;
                double progress = (double) currentIndex / totalSqlCount * 100;
                
                // Calculate estimated time to completion
                long estimatedRemainingSeconds = 0;
                if (progress > 0 && currentIndex > 0) {
                    estimatedRemainingSeconds = Math.round((totalElapsedSeconds / progress) * (100 - progress));
                }
                
                // Format the entire log message to ensure correct formatting
                String logMessage = String.format("SQL execution progress: %d / %d (%.2f%%), Elapsed time: %s, Total Elapsed time: %s, Estimated remaining time: %s", 
                        currentIndex, totalSqlCount, progress, 
                        formatDuration(elapsedSeconds), 
                        formatDuration(totalElapsedSeconds), 
                        formatDuration(estimatedRemainingSeconds));
                LOG.info(logMessage);
            }
            
            // Print final progress
            long totalElapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
            // Format the entire log message to ensure correct formatting
            String finalLogMessage = String.format("SQL execution completed: %d / %d (100%%), Total elapsed time: %s", 
                    totalSqlCount, totalSqlCount, formatDuration(totalElapsedSeconds));
            LOG.info(finalLogMessage);
        }

        /**
         * Format seconds into a human-readable duration string (days, hours, minutes, seconds)
         * 
         * @param seconds Total seconds to format
         * @return Formatted duration string
         */
        private String formatDuration(long seconds) {
            if (seconds <= 0) {
                return "0 seconds";
            }
            
            long days = seconds / (24 * 60 * 60);
            long hours = (seconds % (24 * 60 * 60)) / (60 * 60);
            long minutes = (seconds % (60 * 60)) / 60;
            long secs = seconds % 60;
            
            StringBuilder duration = new StringBuilder();
            
            if (days > 0) {
                duration.append(days).append(" day").append(days > 1 ? "s" : "");
            }
            
            if (hours > 0 || duration.length() > 0) {
                if (duration.length() > 0) {
                    duration.append(", ");
                }
                duration.append(hours).append(" hour").append(hours > 1 ? "s" : "");
            }
            
            if (minutes > 0 || duration.length() > 0) {
                if (duration.length() > 0) {
                    duration.append(", ");
                }
                duration.append(minutes).append(" minute").append(minutes > 1 ? "s" : "");
            }
            
            if (secs > 0 || duration.length() == 0) {
                if (duration.length() > 0) {
                    duration.append(", ");
                }
                duration.append(secs).append(" second").append(secs > 1 ? "s" : "");
            }
            
            return duration.toString();
        }

        /**
         * Build a DataX Record from a ResultSet row.
         * 
         * @param recordSender The RecordSender to create the record
         * @param rs The ResultSet containing the row data
         * @param mandatoryEncoding The encoding to use for string columns (if specified)
         * @return The built Record object
         */
        private Record buildRecord(RecordSender recordSender, ResultSet rs, String mandatoryEncoding) {
            Record record = recordSender.createRecord();
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    int columnType = metaData.getColumnType(i);
                    switch (columnType) {
                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;
                        case Types.FLOAT:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;
                        case Types.BOOLEAN:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;
                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;
                        case Types.BINARY:
                        case Types.VARCHAR:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;
                        case Types.NCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String((rs.getBytes(i) == null ? new byte[0] : rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;
                    }
                }
            } catch (SQLException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "Database query error!", e);
            } catch (UnsupportedEncodingException e) {
                throw DataXException.asDataXException(TDengineReaderErrorCode.ILLEGAL_VALUE, "Illegal mandatory encoding", e);
            }
            return record;
        }
    }


}
