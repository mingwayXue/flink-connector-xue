package com.xue.bigdata.test.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 过滤，类比于 dataStream 的 filter 操作
 */
public class EtlTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceSql = "CREATE TABLE source_table (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.order_number.min' = '1',\n" +
                "  'fields.order_number.max' = '11'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        tableEnv.executeSql("insert into sink_table select * from source_table where order_number = 10");
    }
}
