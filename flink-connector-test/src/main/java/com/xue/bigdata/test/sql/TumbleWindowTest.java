package com.xue.bigdata.test.sql;

import com.xue.bigdata.test.udf.Mod_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TumbleWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 注：sql 的 watermark 类型必须要设置为 TIMESTAMP(3)，如果你的数据源时间戳类型是 13 位 bigint 类型时间戳，可以用 ts AS TO_TIMESTAMP_LTZ(row_time, 3) 将其转换为 TIMESTAMP(3) 类型
        String sourceSql = "CREATE TABLE source_table (\n" +
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
                "    pv BIGINT,\n" +
                "    sum_price BIGINT,\n" +
                "    max_price BIGINT,\n" +
                "    min_price BIGINT,\n" +
                "    uv BIGINT,\n" +
                "    window_start bigint\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        String execSql = "insert into sink_table\n" +
                "select dim,\n" +
                "    sum(bucket_pv) as pv,\n" +
                "    sum(bucket_sum_price) as sum_price,\n" +
                "    max(bucket_max_price) as max_price,\n" +
                "    min(bucket_min_price) as min_price,\n" +
                "    sum(bucket_uv) as uv,\n" +
                "    max(window_start) as window_start\n" +
                "from (\n" +
                "  SELECT dim,\n" +
                "       UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start, \n" +
                "         window_end, \n" +
                "         count(*) as bucket_pv,\n" +
                "         sum(price) as bucket_sum_price,\n" +
                "         max(price) as bucket_max_price,\n" +
                "         min(price) as bucket_min_price,\n" +
                "            -- 计算 uv 数\n" +
                "         count(distinct user_id) as bucket_uv\n" +
                "  FROM TABLE(TUMBLE(\n" +
                "     TABLE source_table\n" +
                "     , DESCRIPTOR(row_time)\n" +
                "     , INTERVAL '60' SECOND))\n" +
                "  GROUP BY window_start, \n" +
                "       window_end,\n" +
                "     dim,\n" +
                "              -- 按照用户 id 进行分桶，防止数据倾斜\n" +
                "     mod(user_id, 1024)\n" +
                ")\n" +
                "group by dim,\n" +
                "   window_start";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.createTemporarySystemFunction("mod", Mod_UDF.class);
        tableEnv.executeSql(execSql);
    }
}
