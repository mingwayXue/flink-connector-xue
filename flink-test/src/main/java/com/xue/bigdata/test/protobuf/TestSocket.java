package com.xue.bigdata.test.protobuf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceTableSql = "CREATE TABLE user_score ("
                + "  name STRING\n"
                + "  , score INT \n"
                + ")\n"
                + "WITH (\n"
                + "  'connector' = 'socket',\n"
                + "  'hostname' = '127.0.0.1',\n"
                + "  'port' = '9999',\n"
                + "  'format' = 'json'\n"
                + ")";

        tEnv.executeSql(sourceTableSql);
        tEnv.executeSql("SELECT name, SUM(score) FROM user_score GROUP BY name").print();
    }
}
