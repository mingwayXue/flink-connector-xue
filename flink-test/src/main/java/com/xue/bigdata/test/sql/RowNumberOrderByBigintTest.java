package com.xue.bigdata.test.sql;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.fs.osshadoop.OSSFileSystemFactory;
import org.apache.flink.fs.shaded.hadoop3.org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.flink.fs.shaded.hadoop3.org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用 row_number = 1 实现去重 --> deduplication
 */
public class RowNumberOrderByBigintTest {
    public static void main(String[] args) throws XaFacade.EmptyXaTransactionException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceSql = "CREATE TABLE source_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    server_timestamp as UNIX_TIMESTAMP()\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.name.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '10'\n"
                + ")";
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    rn BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";
        String execSql = "INSERT INTO sink_table\n"
                + "select user_id,\n"
                + "       name,\n"
                + "       rn\n"
                + "from (\n"
                + "      SELECT\n"
                + "          user_id,\n"
                + "          name,\n"
                + "          row_number() over(partition by user_id order by server_timestamp) as rn\n"
                + "      FROM source_table\n"
                + ")\n"
                + "where rn = 1";
        tableEnv.executeSql(sourceSql);
        // tableEnv.executeSql("select * from source_table").print();
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(execSql);
    }
}
