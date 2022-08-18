package com.xue.bigdata.test.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xue.bigdata.test.util.DebeziumRecord;
import com.xue.bigdata.test.util.HeyteaDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(Test01.class.getClassLoader().getResourceAsStream("application.properties"));
        Configuration configuration = new Configuration();
        // configuration.setString("execution.savepoint.path", "file:///workspace/github/flink-connector-xue/flink-connector-test/src/main/resources/test01/e724fb0ae0ee283fe83e9dfc6847c969/chk-4");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        /*env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///workspace/github/flink-connector-xue/flink-connector-test/src/main/resources/test01");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);*/
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(parameter.get("db.host"))
                .port(3306)
                .databaseList("platform")
                .tableList("platform.fc.*")
                .username(parameter.get("db.username"))
                .password(parameter.get("db.password"))
                .scanNewlyAddedTableEnabled(true)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.latest())
                .includeSchemaChanges(true)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "SOURCE");

        source.printToErr();

        env.execute("Test01");
    }
}
