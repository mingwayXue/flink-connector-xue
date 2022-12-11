package com.xue.bigdata.test.sql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: mingway
 * @date: 2022/12/11 3:44 PM
 */
public class MultiTableInsert01 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(TestSQL01.class.getClassLoader().getResourceAsStream("application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "TestSQL01");


        String t1 = "CREATE TABLE t1 (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.order_number.min' = '1',\n" +
                "  'fields.order_number.max' = '11'\n" +
                ")";
        tableEnv.executeSql(t1);

        String t2 = "CREATE TABLE t2 (\n" +
//                "    order_number BIGINT, " +
                "    price        DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        tableEnv.executeSql(t2);

        String t3 = "CREATE TABLE t3 (\n" +
                "    order_number BIGINT, " +
                "    price        DECIMAL(16,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///xue/workspace/github/flink-connector-xue/flink-test/src/main/resources',\n" +
                "  'format' = 'json'" +
                ")";

        tableEnv.executeSql(t3);

        // tableEnv.executeSql("insert into t3 select * from t1").print();

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("insert into t2 select sum(price) as price from t1 group by order_number");
        statementSet.addInsertSql("insert into t3 select * from t1");

        statementSet.execute();
    }
}
