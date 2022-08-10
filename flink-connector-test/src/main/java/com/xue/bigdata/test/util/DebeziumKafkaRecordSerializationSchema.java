package com.xue.bigdata.test.util;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.OptionalInt;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: mingway
 * @date: 2021/12/1 1:43 下午
 */
public class DebeziumKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<DebeziumRecord> {

    private final SerializationSchema<String> valueSerializationSchema;
    private final Function<DebeziumRecord, String> topicSelector;
    private final FlinkKafkaPartitioner<DebeziumRecord> partitioner;
    private final SerializationSchema<String> keySerializationSchema;

    public DebeziumKafkaRecordSerializationSchema(
            Function<DebeziumRecord, String> topicSelector,
            SerializationSchema<String> valueSerializationSchema,
            @Nullable SerializationSchema<String> keySerializationSchema,
            @Nullable FlinkKafkaPartitioner<DebeziumRecord> partitioner) {
        this.topicSelector = checkNotNull(topicSelector);
        this.valueSerializationSchema = checkNotNull(valueSerializationSchema);
        this.partitioner = partitioner;
        this.keySerializationSchema = keySerializationSchema;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        valueSerializationSchema.open(context);
        if (keySerializationSchema != null) {
            keySerializationSchema.open(context);
        }
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
    }

    /**
     * 自定义序列化方法
     * @param element
     * @param context
     * @param timestamp
     * @return
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(DebeziumRecord element, KafkaSinkContext context, Long timestamp) {
        final String targetTopic = topicSelector.apply(element);
        final byte[] value = valueSerializationSchema.serialize(element.getMsg());
        byte[] key = null;
        if (keySerializationSchema != null) {
            key = keySerializationSchema.serialize(element.getRowKeyVal());
        }
        // 自定义分区
        final OptionalInt partition =
                partitioner != null
                        ? OptionalInt.of(
                        partitioner.partition(
                                element,
                                key,
                                value,
                                targetTopic,
                                context.getPartitionsForTopic(targetTopic)))
                        : OptionalInt.empty();

        return new ProducerRecord<>(
                targetTopic, partition.isPresent() ? partition.getAsInt() : null, key, value);
    }
}
