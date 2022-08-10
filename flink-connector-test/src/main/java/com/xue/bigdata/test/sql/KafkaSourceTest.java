package com.xue.bigdata.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: mingway
 * @date: 2022/7/20 10:52 PM
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql(
                "CREATE TABLE test01 (\n"
                        + "  `f0` STRING,\n"
                        + "  `f1` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'bigdata_test_1',\n"
                        + "  'properties.bootstrap.servers' = '10.6.1.16:9092',\n"
                        + "  'properties.group.id' = 'test01Group',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'format' = 'json'\n"
                        + ")");
        tableEnv.executeSql("select * from test01").print();

        env.execute("KafkaSourceTest");
    }
}
