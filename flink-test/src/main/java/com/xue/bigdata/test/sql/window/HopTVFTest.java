package com.xue.bigdata.test.sql.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Window TVF 方式实现（推荐使用）
 */
public class HopTVFTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE source_table (\n" +
                "    -- 维度数据\n" +
                "    dim STRING,\n" +
                "    -- 用户 id\n" +
                "    user_id BIGINT,\n" +
                "    -- 用户\n" +
                "    price BIGINT,\n" +
                "    -- 事件时间戳\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    -- watermark 设置\n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.dim.length' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '100000',\n" +
                "  'fields.price.min' = '1',\n" +
                "  'fields.price.max' = '100000'\n" +
                ")";
        tableEnv.executeSql(sql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    dim STRING,\n" +
                "    uv BIGINT,\n" +
                "    window_start timestamp(3),\n" +
                "    window_time timestamp(3),\n" +
                "    window_end timestamp(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);
        /*
insert into source_table
select dim,count(distinct user_id) as uv,window_start,window_time,window_end
from TABLE(HOP(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '1' MINUTES, INTERVAL '5' MINUTES))
group by dim,window_start,window_time,window_end
         */
        String execSql = "insert into sink_table\n" +
                "select dim,count(distinct user_id) as uv,window_start,window_time,window_end\n" +
                "from TABLE(HOP(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '1' MINUTES, INTERVAL '5' MINUTES))\n" +
                "group by dim,window_start,window_time,window_end";
        tableEnv.executeSql(execSql);
    }
}
