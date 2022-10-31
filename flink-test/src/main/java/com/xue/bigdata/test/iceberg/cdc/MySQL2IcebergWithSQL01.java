package com.xue.bigdata.test.iceberg.cdc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySQL2IcebergWithSQL01 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(MySQL2IcebergWithSQL01.class.getClassLoader().getResourceAsStream("application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///workspace/github/flink-connector-xue/flink-test/src/main/resources");
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // mysql cdc source
        String sourceSql = "CREATE TABLE fc01 (\n" +
                "id INT,\n" +
                "name STRING,\n" +
                "PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = '" + parameter.get("db.host") + "',\n" +
                "'port' = '3306',\n" +
                "'username' = '" + parameter.get("db.username") + "',\n" +
                "'password' = '" + parameter.get("db.password") + "',\n" +
                "'database-name' = 'platform',\n" +
//                "'scan.startup.mode' = 'latest-offset',\n" +
                "'table-name' = 'fc01')";
        tableEnv.executeSql(sourceSql);

        /*tableEnv.executeSql("show catalogs").print();
        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("show tables").print();*/

        // iceberg catalog
        String catalogSql = "CREATE CATALOG ice_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://" + parameter.get("hive.host") + ":9083',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs://" + parameter.get("hive.host") + ":8020/user/hive/warehouse'\n" +
                ")";
        tableEnv.executeSql(catalogSql);
        tableEnv.executeSql("use catalog ice_catalog");
        tableEnv.executeSql("use ice_db");

        // iceberg create table sql
        String icebergSql = "CREATE TABLE IF NOT EXISTS fc01 (\n" +
                "  id INT NOT NULL,\n" +
                "  name STRING,\n" +
                "  PRIMARY KEY(id) NOT ENFORCED\n" +
                ") with(\n" +
                " 'format-version'='2',\n" +
                " 'write.metadata.delete-after-commit.enabled'='true',\n" +
                " 'write.metadata.previous-versions-max'='3',\n" +
                " 'write.distribution-mode'='hash',\n" +
                " 'write.upsert.enabled' = 'true'\n" +
                ")";
        tableEnv.executeSql(icebergSql);

        // execute
        tableEnv.executeSql("insert into ice_catalog.ice_db.fc01 select * from default_catalog.default_database.fc01");

        // tableEnv.executeSql("select * from fc01").print();
    }
}
