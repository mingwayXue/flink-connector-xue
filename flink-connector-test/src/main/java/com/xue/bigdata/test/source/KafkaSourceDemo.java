package com.xue.bigdata.test.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: mingway
 * @date: 2022/2/24 9:07 下午
 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.24.19.17:9092,172.24.19.19:9092,172.24.19.20:9092")
                .setTopics("test_xue_01")
                .setGroupId("test-xue")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setClientIdPrefix("flink_")
                .setProperty("partition.discovery.interval.ms", "10000")
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "1000")
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafkaSource");

        dataStreamSource.print();

        env.execute("testApp");
    }
}
