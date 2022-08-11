package com.xue.bigdata.test.cdc;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestSQL01 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(TestSQL01.class.getClassLoader().getResourceAsStream("application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "TestSQL01");

        String createSourceSql = "CREATE TABLE mt_orders (\n" +
                "  `id` BIGINT,\n" +
                "  `shop_id` INT,\n" +
                "  `shop_name` STRING,\n" +
                "  `no` STRING,\n" +
                "  `created_at` BIGINT,\n" +
                "  `updated_at` BIGINT,\n" +
                "  `dispatched_at` BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_db_production__mt_orders',\n" +
                "  'properties.bootstrap.servers' = '" + parameter.get("bootstrap.servers") + "',\n" +
                "  'properties.group.id' = 'test-xue',\n" +
                "  'scan.startup.mode' = 'timestamp',\n" +
                "  'scan.startup.timestamp-millis' = '1660096800000',\n" +
                "  'format' = 'debezium-json'\n" +
                ")";
        tableEnv.executeSql(createSourceSql);

        tableEnv.executeSql("select * from mt_orders where `no` = '151976173342425047'").print();
    }
}
