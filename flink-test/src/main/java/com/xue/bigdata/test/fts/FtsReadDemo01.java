package com.xue.bigdata.test.fts;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FtsReadDemo01 {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        /*env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);*/
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // String wh = "oss://heytea-bigdata-oss/bigdata-oss/fts";
        String wh = "file:///tmp/table_store";

        String printSql = "CREATE TABLE print (\n" +
                "    order_number BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "    price DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(printSql);

        String execSql1 = "CREATE CATALOG fts WITH (\n" +
                "  'type'='table-store',\n" +
                "  'warehouse'='" + wh + "'\n" +
                ")";

        String execSql2 = "use catalog fts";

        String execSql3 = "CREATE TABLE if not exists t2 (\n" +
                "    order_number BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "    price DECIMAL(16,2)\n" +
                ") with (" +
//                " 'changelog-producer' = 'full-compaction' " +
                " 'log.system' = 'kafka',\n" +
                " 'kafka.bootstrap.servers' = '10.6.1.16:9092',\n" +
                " 'scan.mode'='latest'," +
                " 'log.consistency'='eventual'," +
                " 'kafka.topic' = 'bigdata_test_1'" +
                ")";

        // String execSql5 = "select * from t2";

        tableEnv.executeSql(execSql1);
        tableEnv.executeSql(execSql2);
        tableEnv.executeSql(execSql3);

        tableEnv.executeSql("insert into default_catalog.default_database.print select * from t2");


        // tableEnv.executeSql(execSql5).print();

    }
}
