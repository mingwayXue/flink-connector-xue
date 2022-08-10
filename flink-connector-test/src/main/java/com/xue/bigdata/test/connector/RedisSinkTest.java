package com.xue.bigdata.test.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: mingway
 * @date: 2022/8/8 10:48 PM
 */
public class RedisSinkTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Row> rowDataStreamSource = env.addSource(new CustomDefinedSource());
        Table sourceTable = tableEnv.fromDataStream(rowDataStreamSource, Schema.newBuilder().columnByExpression("proctime", "PROCTIME()").build());
        tableEnv.createTemporaryView("sourceTable", sourceTable);

        String createSql = "CREATE TABLE sinkTable (\n" +
                "    key STRING, -- redis key，第 1 列为 key\n" +
                "    `value` STRING -- redis value，第 2 列为 value\n" +
                ") WITH (\n" +
                "  'connector' = 'redis', -- 指定 connector 是 redis 类型的\n" +
                "  'hostname' = 'test-redis-qcloud.heyteago.com', -- redis server ip\n" +
                "  'port' = '6391', -- redis server 端口\n" +
                "  'password' = 'itRK6YHBKBd2ETu8', " +
                "  'write.mode' = 'string' -- 指定使用 redis `set key value`\n" +
                ")";
        tableEnv.executeSql(createSql);

        String sinkSql = "INSERT INTO sinkTable\n" +
                "SELECT o.f0 as key, o.f1 as `value`\n" +
                "FROM sourceTable AS o";
        tableEnv.executeSql(sinkSql).print();


        env.execute();
    }

    private static class CustomDefinedSource implements SourceFunction<Row>, ResultTypeQueryable<Row> {

        private volatile boolean isCancel;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {

            while (!this.isCancel) {

                sourceContext.collect(Row.of("a1", "b1", 1L));

                Thread.sleep(1000L);
            }

        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class),
                    TypeInformation.of(Long.class));
        }
    }
}
