package com.xue.bigdata.test.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 聚合，
 */
public class EtlTest03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceSql = "CREATE TABLE source_table (\n" +
                "    order_id STRING,\n" +
                "    price BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.order_id.length' = '1',\n" +
                "  'fields.price.min' = '1',\n" +
                "  'fields.price.max' = '1000000'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    order_id STRING,\n" +
                "    count_result BIGINT,\n" +
                "    sum_result BIGINT,\n" +
                "    avg_result DOUBLE,\n" +
                "    min_result BIGINT,\n" +
                "    max_result BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        tableEnv.executeSql("insert into sink_table\n" +
                "select order_id,\n" +
                "       count(*) as count_result,\n" +
                "       sum(price) as sum_result,\n" +
                "       avg(price) as avg_result,\n" +
                "       min(price) as min_result,\n" +
                "       max(price) as max_result\n" +
                "from source_table\n" +
                "group by order_id");
    }
}
