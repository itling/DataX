package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.utils.DateTimeUtils;

public class TDengineJdbcVersionTest {
    public static void main(String[] args) {
        // Test Utils.escapeSingleQuota
        try {
            String result = Utils.escapeSingleQuota("test'quote");
            System.out.println("Utils.escapeSingleQuota exists: " + result);
        } catch (Exception e) {
            System.out.println("Utils.escapeSingleQuota error: " + e.getMessage());
        }

        // Test DateTimeUtils.parseTimestamp
        try {
            long result = DateTimeUtils.parseTimestamp("2023-01-01 00:00:00", null).getTime();
            System.out.println("DateTimeUtils.parseTimestamp exists: " + result);
        } catch (Exception e) {
            System.out.println("DateTimeUtils.parseTimestamp error: " + e.getMessage());
        }
    }
}