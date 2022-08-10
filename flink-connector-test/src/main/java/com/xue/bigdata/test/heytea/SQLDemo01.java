package com.xue.bigdata.test.heytea;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: mingway
 * @date: 2021/12/22 2:33 下午
 */
public class SQLDemo01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);

        String sql = "select CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP";

        tableEnv.executeSql(sql).print();

        /*String createSql1 = "CREATE TABLE st_trade (\n" +
                "         pid bigint,\n" +
                "         biz_date String,\n" +
                "         trade_no String,\n"+
                "         trade_time bigint,\n"+
                "         trade_amount DECIMAL,\n"+
                "         primary key (pid)  NOT ENFORCED \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = '172.24.19.36:9092,172.24.19.37:9092,172.24.19.38:9092',\n" +
                "    'topic' = 'ods_hex_pos_tradedata__st_trade',\n" +
                "    'value.fields-include' = 'ALL'," +
                "    'scan.startup.mode' = 'latest-offset', \n" +
                "    'format' = 'debezium-json' \n" +
                ")";
        tableEnv.executeSql(createSql1);

        tableEnv.executeSql("select *,FROM_UNIXTIME(trade_time/1000) from st_trade").print();*/

    }
}
