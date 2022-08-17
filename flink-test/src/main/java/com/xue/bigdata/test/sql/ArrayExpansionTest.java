package com.xue.bigdata.test.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ArrayExpansionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE show_log_table (\n" +
                "    log_id BIGINT,\n" +
                "    show_params ARRAY<STRING>\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.log_id.min' = '1',\n" +
                "  'fields.log_id.max' = '10'\n" +
                ")";
        tableEnv.executeSql(sql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    log_id BIGINT,\n" +
                "    show_param STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        String execSql = "INSERT INTO sink_table\n" +
                "SELECT\n" +
                "    s.log_id,\n" +
                "    t.show_param as show_param\n" +
                "FROM show_log_table s\n" +
                "-- array 炸开语法\n" +
                "CROSS JOIN UNNEST(s.show_params) AS t (show_param)";
        tableEnv.executeSql(execSql);
    }
}
