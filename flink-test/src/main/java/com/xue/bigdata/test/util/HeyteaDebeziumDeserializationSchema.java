package com.xue.bigdata.test.util;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: mingway
 * @date: 2021/11/30 5:46 下午
 */
public class HeyteaDebeziumDeserializationSchema implements DebeziumDeserializationSchema<DebeziumRecord> {

    private static final long serialVersionUID = 1L;

    /*private transient JsonConverter jsonConverter;

    private final Boolean includeSchema;*/

    private final String CHARACTER = ",";

    private final String topicPrefix = "mysql_binlog_source.";

    private final String topicReplacement = "ods_";

    private List<String> groupList = new ArrayList<>();

    public HeyteaDebeziumDeserializationSchema() {
    }

    public HeyteaDebeziumDeserializationSchema(List<String> groupList) {
        this.groupList = groupList;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<DebeziumRecord> out) throws Exception {
        HeyteaJsonConverter jsonConverter = new HeyteaJsonConverter();

        byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());

        DebeziumRecord debeziumRecord = new DebeziumRecord();
        // 根据规则，设置 topic
        if (groupList.size() > 0) {
            for (String prefix : groupList) {
                if (record.topic().contains(prefix)) {
                    debeziumRecord.setTopic(topicReplacement + prefix.replace(".", "__"));
                    break;
                }
            }
        } else {
            debeziumRecord.setTopic(record.topic().replace(topicPrefix, topicReplacement).replace(".", "__"));
        }
        // 提取 key 和 topic
        if (record.keySchema() != null && record.keySchema().fields().size() > 0) {
            List<String> keys = record.keySchema().fields().stream().map(f -> f.name()).collect(Collectors.toList());
            Struct keyStruct = (Struct) record.key();
            debeziumRecord.setRowKey(keys.stream().collect(Collectors.joining(CHARACTER)));
            debeziumRecord.setRowKeyVal(keys.stream().map(s -> String.valueOf(keyStruct.get(s))).collect(Collectors.joining(CHARACTER)));
        } else {
            debeziumRecord.setRowKey("");
            debeziumRecord.setRowKeyVal("");
        }
        debeziumRecord.setMsg(new String(bytes));

        out.collect(debeziumRecord);
    }

    @Override
    public TypeInformation<DebeziumRecord> getProducedType() {
        return TypeInformation.of(DebeziumRecord.class);
    }
}
