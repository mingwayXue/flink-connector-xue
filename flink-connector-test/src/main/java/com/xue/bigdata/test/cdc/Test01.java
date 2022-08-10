//package com.xue.bigdata.test.cdc;
//
//
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
///**
// * @author: mingway
// * @date: 2022/7/18 11:05 PM
// */
//public class Test01 {
//    public static void main(String[] args) throws Exception {
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("10.21.4.17")
//                .port(3306)
//                .databaseList("platform") // set captured database
//                .tableList("platform.fc01") // set captured table
//                .username("platform")
//                .password("platform@Heytea2021")
//                .splitSize(1)
//                .serverId("5401")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MYSQL_SOURCE");
//
//        dataStreamSource.printToErr();
//
//        env.execute("TT");
//
//    }
//}
