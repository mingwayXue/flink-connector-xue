package com.xue.bigdata.test.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 去重操作：select distinct 是通过 group aggregate 实现的。先按 keyby，然后 aggregate
 */
public class EtlTest02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceSql = "CREATE TABLE source_table (\n" +
                "    string_field STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.string_field.length' = '3'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    string_field STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        tableEnv.executeSql("insert into sink_table select distinct string_field from source_table");
    }
}
