package com.xue.bigdata.test.cdc;//package com.xue.demo.cdc;
//
//import com.heytea.cdc.connectors.mysql.source.MySqlSource;
//import com.heytea.cdc.connectors.mysql.table.StartupOptions;
//import com.xue.demo.util.DebeziumKafkaRecordSerializationSchema;
//import com.xue.demo.util.DebeziumRecord;
//import com.xue.demo.util.HeyteaDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.sink.TopicSelector;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
//
//import java.util.Properties;
//
///**
// * @author: mingway
// * @date: 2021/11/29 10:56 上午
// */
//public class CdcTable1 {
//
//    public static void main(String[] args) throws Exception {
//        MySqlSource<DebeziumRecord> mySqlSource = MySqlSource.<DebeziumRecord>builder()
//                .hostname("120.79.208.200")
//                .port(3306)
//                .databaseList("xue_demo") // set captured database
//                .tableList("xue_demo.trade,xue_demo.order") // set captured table
//                .username("root")
//                .password("ivan7788Xue!")
//                .startupOptions(StartupOptions.earliest())
//                .deserializer(new HeyteaDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////        env.enableCheckpointing(1000 * 30, CheckpointingMode.EXACTLY_ONCE);
////        env.setStateBackend(new HashMapStateBackend());
////        env.getCheckpointConfig().setCheckpointStorage("file:///xue/documents/heytea/gitlab/heytea-flink-job/xue-demo/src/main/resources/");
////        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
////        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setParallelism(1);
//
//        DataStreamSource<DebeziumRecord> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MYSQL_SOURCE");
//
//        // dataStreamSource.print();
//
//        /*SingleOutputStreamOperator<String> targetStream = dataStreamSource.map(new MapFunction<DebeziumRecord, String>() {
//            @Override
//            public String map(DebeziumRecord debeziumRecord) throws Exception {
//                return debeziumRecord.getMsg();
//            }
//        });*/
//
//        Properties properties = new Properties();
//        // 使用 EXACTLY_ONCE 语义时需要设置事务超时时间不得超过 kafka 系统的事务超时时间
////        properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");
//        properties.setProperty("group.id", "test1");
//        KafkaSink<DebeziumRecord> kafkaSink= KafkaSink.<DebeziumRecord>builder()
//                .setBootstrapServers("172.24.19.17:9092,172.24.19.19:9092,172.24.19.20:9092")
//                .setKafkaProducerConfig(properties)
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
////                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .build();
//
//        dataStreamSource.sinkTo(kafkaSink);
//
//        /*Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "172.24.19.17:9092,172.24.19.19:9092,172.24.19.20:9092");
//        properties.setProperty("group.id", "test001");
//        targetStream.addSink(new FlinkKafkaProducer<String>("flink_cdc_t2", new SimpleStringSchema(), properties));*/
//
//
//        env.execute("CdcTable1_APP");
//    }
//}
