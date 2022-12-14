package com.xue.bigdata.test.fts;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: mingway
 * @date: 2022/12/11 4:37 PM
 */
public class FtsDoubleWrite01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "TestSQL01");

        String sql1 = "CREATE TABLE t1 (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" + // 每秒生成的数据行数
                "  'fields.order_number.min' = '1',\n" +
                "  'fields.order_number.max' = '1000'\n" +
                ")";
        tableEnv.executeSql(sql1);

        // 10.6.1.16:9092
        // bigdata_test_1
        String sql2 = "CREATE CATALOG fts WITH (\n" +
                "  'type'='table-store',\n" +
                "  'warehouse'='file:///tmp/table_store'\n" +
                ")";
        tableEnv.executeSql(sql2);
        tableEnv.executeSql("use catalog fts");

        String sql3 = "CREATE TABLE if not exists t2 (\n" +
                "    order_number BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "    price DECIMAL(16,2)\n" +
                ") with (\n" +
                " 'log.system' = 'kafka',\n" +
                " 'kafka.bootstrap.servers' = '10.6.1.16:9092',\n" +
                " 'kafka.transaction.timeout.ms' = '600000',\n" +
                " 'log.consistency'='eventual',\n" +
                " 'kafka.topic' = 'bigdata_test_1'" +
                ")";
        tableEnv.executeSql(sql3);

        tableEnv.executeSql("insert into t2 select * from default_catalog.default_database.t1");

    }
}
