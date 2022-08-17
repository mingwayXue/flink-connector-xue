package com.xue.bigdata.test.sql.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 仅有 Window TVF 方式实现（推荐使用）
 */
public class CumulateTVFTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE source_table (\n" +
                "    -- 用户 id\n" +
                "    user_id BIGINT,\n" +
                "    -- 用户\n" +
                "    money BIGINT,\n" +
                "    -- 事件时间戳\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    -- watermark 设置\n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '100000',\n" +
                "  'fields.money.min' = '1',\n" +
                "  'fields.money.max' = '100000'\n" +
                ")";
        tableEnv.executeSql(sql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    window_start timestamp(3),\n" +
                "    window_end timestamp(3),\n" +
                "    sum_money bigint,\n" +
                "    count_distinct_id bigint\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        /*
insert into sink_table
select window_start,window_end,sum(money) as sum_money,count(distinct id) as count_distinct_id
from TABLE(CUMULATE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '60' SECOND, INTERVAL '1' DAY))
group by window_start,window_end
         */
        String execSql = "insert into sink_table\n" +
                "select window_start,window_end,sum(money) as sum_money,count(distinct user_id) as count_distinct_id\n" +
                "from TABLE(CUMULATE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '60' SECOND, INTERVAL '1' DAY))\n" +
                "group by window_start,window_end";
        tableEnv.executeSql(execSql);
    }
}
