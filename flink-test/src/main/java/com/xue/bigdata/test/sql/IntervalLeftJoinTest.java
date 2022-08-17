package com.xue.bigdata.test.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * interval left join
 */
public class IntervalLeftJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql1 = "CREATE TABLE show_log (\n"
                + "    log_id BIGINT,\n"
                + "    show_params STRING,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.show_params.length' = '1',\n"
                + "  'fields.log_id.min' = '5',\n"
                + "  'fields.log_id.max' = '15'\n"
                + ")";
        String sql2  = "CREATE TABLE click_log (\n"
                + "    log_id BIGINT,\n"
                + "    click_params STRING,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time\n"
                + ")\n"
                + "WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.click_params.length' = '1',\n"
                + "  'fields.log_id.min' = '1',\n"
                + "  'fields.log_id.max' = '10'\n"
                + ")";
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    s_id BIGINT,\n"
                + "    s_params STRING,\n"
                + "    c_id BIGINT,\n"
                + "    c_params STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";
        String execSql = "INSERT INTO sink_table\n"
                + "SELECT\n"
                + "    show_log.log_id as s_id,\n"
                + "    show_log.show_params as s_params,\n"
                + "    click_log.log_id as c_id,\n"
                + "    click_log.click_params as c_params\n"
                + "FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id\n"
                + "AND show_log.row_time BETWEEN click_log.row_time - INTERVAL '5' SECOND AND click_log.row_time + INTERVAL '5' SECOND";

        tableEnv.executeSql(sql1);
        tableEnv.executeSql(sql2);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(execSql);
    }
}
