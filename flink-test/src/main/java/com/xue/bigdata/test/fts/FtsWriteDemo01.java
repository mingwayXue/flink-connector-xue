package com.xue.bigdata.test.fts;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FtsWriteDemo01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
         env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///workspace/github/flink-connector-xue/flink-test/src/main/resources/fts");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // String wh = "oss://heytea-bigdata-oss/bigdata-oss/fts";
        String wh = "file:/tmp/table_store";

        String execSql1 = "CREATE CATALOG fts WITH (\n" +
                "  'type'='table-store',\n" +
                "  'warehouse'='" + wh + "'\n" +
                ")";

        String execSql2 = "use catalog fts";

        String execSql3 = "CREATE TABLE if not exists word_count (\n" +
                "    word STRING PRIMARY KEY NOT ENFORCED,\n" +
                "    cnt BIGINT\n" +
                ")";

        /*String execSql4 = "CREATE TEMPORARY TABLE word_table (\n" +
                "    word STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'fields.word.length' = '1'\n" +
                ")";

        String execSql5 = "INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word";*/

        String execSql4 = "insert into word_count values ('A', 1)";

        String execSql5 = "select * from word_count";

        tableEnv.executeSql(execSql1);
        tableEnv.executeSql(execSql2);
        tableEnv.executeSql(execSql3);
        tableEnv.executeSql(execSql4);
        // tableEnv.executeSql(execSql5);

    }
}
