package com.xue.bigdata.test.datastream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @author: mingway
 * @date: 2022/12/13 11:37 PM
 */
public class TestChangeLogStream01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**
         * {"id":1,"name":"a"}
         * {"id":2,"name":"b"}
         * {"id":3,"name":"c"}
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.6.1.16:9092")
                .setTopics("bigdata_test_1")
                .setGroupId("test-xue")
                // .setStartingOffsets(OffsetsInitializer.latest())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setClientIdPrefix("flink_")
                .setProperty("partition.discovery.interval.ms", "10000")
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "1000")
                .build();

        DataStreamSource<String> kafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KAFKA");

        DataStream<Row> rowStream = kafka.map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        return JSONObject.parseObject(s);
                    }
                }).keyBy(x -> x.getInteger("id"))
                .process(new KeyedProcessFunction<Integer, JSONObject, Row>() {

                    private ValueState<JSONObject> jsonState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>("json-state", JSONObject.class);
                        jsonState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject json, KeyedProcessFunction<Integer, JSONObject, Row>.Context ctx, Collector<Row> out) throws Exception {
                        Row row = Row.withNames();
//                        Row row;
                        if (Objects.isNull(jsonState.value())) {
//                            row = Row.of(RowKind.INSERT, json.get("id"), json.get("name"));
                            row.setKind(RowKind.INSERT);
                        } else {
//                            row = Row.of(RowKind.UPDATE_AFTER, json.get("id"), json.get("name"));
                            row.setKind(RowKind.UPDATE_AFTER);
                        }
                        jsonState.update(json);
                        row.setField("id", json.get("id"));
                        row.setField("name", json.get("name"));
                        out.collect(row);
                    }
                }).returns(Types.ROW_NAMED(new String[]{"id", "name"}, Types.INT, Types.STRING));
        // .returns(new RowTypeInfo(TypeInformation.of(RowKind.class), Types.INT, Types.STRING));

        // rowStream.printToErr();


        Table table1 = tableEnv.fromChangelogStream(rowStream,
                Schema.newBuilder()
//                        .primaryKey("id")
                        .column("id", "INT")
                        .column("name", "STRING")
                        .build());

        /*table1.printSchema();
        table1.execute().print();*/


        tableEnv.createTemporaryView("test", table1);
        tableEnv.executeSql("select name,sum(id) from test group by name").print();

        // env.execute("TestChangeLogStream01");
    }
}
