package com.xue.bigdata.test.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.xue.bigdata.test.util.DebeziumRecord;
import com.xue.bigdata.test.util.HeyteaDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(Test01.class.getClassLoader().getResourceAsStream("application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///bigdata/flink-1.14.4/chk-dirs/test01");
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);

        MySqlSource<DebeziumRecord> mySqlSource = MySqlSource.<DebeziumRecord>builder()
                .hostname(parameter.get("db.host"))
                .port(3306)
                .databaseList("platform")
                .tableList("platform.fc01")
                .username(parameter.get("db.username"))
                .password(parameter.get("db.password"))
                .scanNewlyAddedTableEnabled(true)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .deserializer(new HeyteaDebeziumDeserializationSchema())
                .build();

        DataStreamSource<DebeziumRecord> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "SOURCE");

        source.printToErr();

        env.execute("Test01");
    }
}
