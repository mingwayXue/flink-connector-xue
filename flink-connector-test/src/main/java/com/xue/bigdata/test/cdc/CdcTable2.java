package com.xue.bigdata.test.cdc;//package com.xue.demo.cdc;
//
//import com.heytea.cdc.connectors.mysql.source.MySqlSource;
//import com.heytea.cdc.connectors.mysql.table.StartupOptions;
//import com.heytea.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import java.util.Properties;
//
///**
// * @author: mingway
// * @date: 2021/11/29 10:56 上午
// */
//public class CdcTable2 {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
//
//        String s = "CREATE TABLE table_a (\n" +
//                "    id int,\n" +
//                "    name varchar,\n" +
//                "    created_at bigint,\n" +
//                "    t as TO_TIMESTAMP(FROM_UNIXTIME(created_at/1000,'yyyy-MM-dd HH:mm:ss'))\n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                "    'properties.bootstrap.servers' = '172.24.19.19:9092,172.24.19.20:9092,172.24.19.17:9092',\n" +
//                "    'topic' = 'ods_xue_demo__table_a',\n" +
//                "    'value.fields-include' = 'ALL',\n" +
//                "    'scan.startup.mode' = 'earliest-offset', \n" +
//                "    'format' = 'debezium-json' \n" +
//                ")";
//        tableEnv.executeSql(s);
//
//
//        tableEnv.executeSql("select * from table_a").print();
//
//
//        /*String createSql1 = "CREATE TABLE flink_cdc_table1 (\n" +
//                "         id int,\n" +
//                "         name String,\n" +
//                "          primary key (id)  NOT ENFORCED \n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                "    'properties.bootstrap.servers' = '172.24.19.19:9092,172.24.19.20:9092,172.24.19.17:9092',\n" +
//                "    'topic' = 'ods_xue_demo.flink_cdc_table1',\n" +
//                "    'value.fields-include' = 'ALL'," +
//                "    'scan.startup.mode' = 'earliest-offset', \n" +
//                "    'format' = 'debezium-json' \n" +
//                ")";
//        tableEnv.executeSql(createSql1);
//
//        String createSql3 = "CREATE TABLE flink_cdc_table3 (\n" +
//                "         id int,\n" +
//                "         uv bigint,\n" +
//                "          primary key (id)  NOT ENFORCED \n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                "    'properties.bootstrap.servers' = '172.24.19.19:9092,172.24.19.20:9092,172.24.19.17:9092',\n" +
//                "    'topic' = 'ods_xue_demo.flink_cdc_table3',\n" +
//                "    'value.fields-include' = 'ALL'," +
//                "    'scan.startup.mode' = 'earliest-offset', \n" +
//                "    'format' = 'debezium-json' \n" +
//                ")";
//        tableEnv.executeSql(createSql3);
//
//
//        tableEnv.executeSql("select a.id,b.name,a.uv from flink_cdc_table3 a\n" +
//                "inner join flink_cdc_table1 b on a.id = b.id").print();*/
//
//
//        env.execute("CdcTable2_APP");
//    }
//}
