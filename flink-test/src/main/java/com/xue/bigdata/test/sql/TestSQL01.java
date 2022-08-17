package com.xue.bigdata.test.sql;

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
                "  `time_ltz` AS TO_TIMESTAMP_LTZ(`created_at`, 3)," +
                // "  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" + // 指定事件事件
                "  time_proc AS PROCTIME()" + // 指定处理时间
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_db_production__mt_orders',\n" +
                "  'properties.bootstrap.servers' = '" + parameter.get("bootstrap.servers") + "',\n" +
                "  'properties.group.id' = 'test-xue',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
//                "  'scan.startup.mode' = 'timestamp',\n" +
//                "  'scan.startup.timestamp-millis' = '1660096800000',\n" +
                "  'format' = 'debezium-json'\n" +
                ")";
        tableEnv.executeSql(createSourceSql);

        tableEnv.executeSql("select * from mt_orders").print();
        // tableEnv.executeSql("SELECT TUMBLE_START(time_ltz, INTERVAL '1' MINUTE), COUNT(DISTINCT shop_id) FROM mt_orders GROUP BY TUMBLE(time_ltz, INTERVAL '1' MINUTE)").print();
    }
}
