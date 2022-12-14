package com.xue.bigdata.test.fts;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: mingway
 * @date: 2022/12/11 6:37 PM
 */
public class FtsKafkaRead01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "FtsKafkaRead01");

        String sql1 = "CREATE TABLE t2 (\n" +
                "    order_number BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "    price DECIMAL(16,2)\n" +
                ") with (" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'bigdata_test_1',\n" +
                " 'properties.bootstrap.servers' = '10.6.1.16:9092',\n" +
                " 'format' = 'debezium-json',\n" +
                " 'scan.startup.mode' = 'latest-offset'" +
                ")";
        tableEnv.executeSql(sql1);

        tableEnv.executeSql("select * from t2").print();
    }
}
