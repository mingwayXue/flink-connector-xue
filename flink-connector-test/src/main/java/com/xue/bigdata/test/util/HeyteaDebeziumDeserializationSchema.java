package com.xue.bigdata.test.util;//package com.xue.demo.util;
//
//import com.heytea.cdc.debezium.DebeziumDeserializationSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;
//
//import java.util.List;
//import java.util.stream.Collectors;
//
///**
// * @author: mingway
// * @date: 2021/11/30 5:46 下午
// */
//public class HeyteaDebeziumDeserializationSchema implements DebeziumDeserializationSchema<DebeziumRecord> {
//
//    private static final long serialVersionUID = 1L;
//
//    /*private transient JsonConverter jsonConverter;
//
//    private final Boolean includeSchema;*/
//
//    private final String CHARACTER = ",";
//
//    private final String topicPrefix = "mysql_binlog_source.";
//
//    private final String topicReplacement = "ods_";
//
//    public HeyteaDebeziumDeserializationSchema() {
//    }
//
//    /*public HeyteaDebeziumDeserializationSchema() {
//        this(false);
//    }
//
//    public HeyteaDebeziumDeserializationSchema(Boolean includeSchema) {
//        this.includeSchema = includeSchema;
//    }*/
//
//    @Override
//    public void deserialize(SourceRecord record, Collector<DebeziumRecord> out) throws Exception {
//        /*if (this.jsonConverter == null) {
//            this.jsonConverter = new JsonConverter();
//            HashMap<String, Object> configs = new HashMap(2);
//            configs.put("converter.type", ConverterType.VALUE.getName());
//            configs.put("schemas.enable", this.includeSchema);
//            this.jsonConverter.configure(configs);
//        }*/
//        HeyteaJsonConverter jsonConverter = new HeyteaJsonConverter();
//
//        byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
//        // 提取 key 和 topic
//        DebeziumRecord debeziumRecord = new DebeziumRecord();
//        debeziumRecord.setTopic(record.topic().replace(topicPrefix, topicReplacement).replace(".", "__"));
//        if (record.keySchema() != null && record.keySchema().fields().size() > 0) {
//            List<String> keys = record.keySchema().fields().stream().map(f -> f.name()).collect(Collectors.toList());
//            Struct keyStruct = (Struct) record.key();
//            debeziumRecord.setRowKey(keys.stream().collect(Collectors.joining(CHARACTER)));
//            debeziumRecord.setRowKeyVal(keys.stream().map(s -> String.valueOf(keyStruct.get(s))).collect(Collectors.joining(CHARACTER)));
//        } else {
//            debeziumRecord.setRowKey("");
//            debeziumRecord.setRowKeyVal("");
//        }
//        debeziumRecord.setMsg(new String(bytes));
//
//        out.collect(debeziumRecord);
//    }
//
//    @Override
//    public TypeInformation<DebeziumRecord> getProducedType() {
//        return TypeInformation.of(DebeziumRecord.class);
//    }
//}
