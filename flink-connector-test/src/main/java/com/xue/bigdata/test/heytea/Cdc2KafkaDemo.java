package com.xue.bigdata.test.heytea;//package com.xue.demo.heytea;
//
//import com.heytea.cdc.connectors.mysql.source.MySqlSource;
//import com.heytea.cdc.connectors.mysql.table.StartupOptions;
//import com.xue.demo.util.DebeziumKafkaRecordSerializationSchema;
//import com.xue.demo.util.DebeziumRecord;
//import com.xue.demo.util.HeyteaDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.sink.TopicSelector;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
//
///**
// * MySQL数据CDC到kafka案例
// * @author: mingway
// * @date: 2021/12/20 5:49 下午
// */
//public class Cdc2KafkaDemo {
//    public static void main(String[] args) throws Exception {
//        // env
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        /*
//        env.enableCheckpointing(1000 * 30, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///xue/documents/heytea/gitlab/heytea-flink-job/xue-demo/src/main/resources/");
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        */
//        env.setParallelism(1);
//
//
//        MySqlSource<DebeziumRecord> mySQLSource = MySqlSource.<DebeziumRecord>builder()
//                .hostname("172.24.19.27")
//                .port(33006)
//                .databaseList("test") // set captured database
//                .tableList("test.user_demo") // set captured table
//                .username("test1")
//                .password("heytea2021")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new HeyteaDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//
//        // source
//        DataStreamSource<DebeziumRecord> dataStreamSource = env.fromSource(mySQLSource, WatermarkStrategy.noWatermarks(), "MYSQL_SOURCE");
//
//        // transformation
//        SingleOutputStreamOperator<DebeziumRecord> mapStream = dataStreamSource.map(new MapFunction<DebeziumRecord, DebeziumRecord>() {
//            @Override
//            public DebeziumRecord map(DebeziumRecord record) throws Exception {
//                System.out.println(record);
//                return record;
//            }
//        });
//
//        // sink
//        KafkaSink<DebeziumRecord> kafkaSink= KafkaSink.<DebeziumRecord>builder()
//                .setBootstrapServers("172.24.19.17:9092,172.24.19.19:9092,172.24.19.20:9092")
//                .setRecordSerializer(new DebeziumKafkaRecordSerializationSchema(
//                        new TopicSelector<DebeziumRecord>() {
//                            @Override
//                            public String apply(DebeziumRecord record) {
//                                return record.getTopic();
//                            }
//                        },
//                        new SimpleStringSchema(),
//                        new SimpleStringSchema(),
//                        new FlinkKafkaPartitioner<DebeziumRecord>() {
//                            @Override
//                            public int partition(DebeziumRecord record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
//                                System.out.println("分区:" + record.getRowKeyVal().hashCode() % partitions.length);
//                                System.out.println("分区数据:" + record.toString());
//                                return record.getRowKeyVal().hashCode() % partitions.length;
//                            }
//                        }
//                ))
//                .build();
//        dataStreamSource.sinkTo(kafkaSink);
//
//        // execute
//        env.execute("Cdc2KafkaDemoJob");
//    }
//}
