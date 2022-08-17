package com.xue.bigdata.test.sql.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Window TVF 方式实现（推荐使用）
 */
public class TumbleTVFTest {
    public static void main(String[] args) throws Exception {
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
        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    dim STRING,\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3),\n" +
//                "    window_start bigint,\n" +
                "    pv BIGINT,\n" +
                "    sum_price BIGINT,\n" +
                "    max_price BIGINT,\n" +
                "    min_price BIGINT,\n" +
                "    uv BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        String execSql = "insert into sink_table\n" +
                "select\n" +
                "    dim\n" +
                "    ,window_start\n" +
                "    ,window_end\n" +
                "    ,count(*) as pv\n" +
                "    ,sum(price) as sum_price\n" +
                "    ,max(price) as max_price\n" +
                "    ,min(price) as min_price\n" +
                "    ,count(distinct user_id) as uv\n" +
                "from TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '60' SECOND))\n" +
                "group by window_start, window_end, window_time, dim";
        /*
insert into sink_table
select
    dim
    ,window_start
    ,window_end
    ,count(*) as pv
    ,sum(price) as sum_price
    ,max(price) as max_price
    ,min(price) as min_price
    ,count(distinct user_id) as uv
from TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '60' SECOND))
group by window_start, window_end, window_time, dim
         */

        tableEnv.executeSql(sql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(execSql);
    }
}
