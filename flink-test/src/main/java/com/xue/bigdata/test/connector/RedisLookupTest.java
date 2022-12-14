package com.xue.bigdata.test.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
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
 * @date: 2022/8/4 8:27 AM
 */
public class RedisLookupTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(RedisLookupTest.class.getClassLoader().getResourceAsStream("application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Row> rowDataStreamSource = env.addSource(new CustomDefinedSource());

        Table sourceTable = tableEnv.fromDataStream(rowDataStreamSource, Schema.newBuilder().columnByExpression("proctime", "PROCTIME()").build());

        tableEnv.createTemporaryView("sourceTable", sourceTable);
        // test: "redis://test-redis-qcloud.heyteago.com:6391"       pwd: itRK6YHBKBd2ETu8
        String createSql = "CREATE TABLE dimTable (\n" +
                "    name STRING,\n" +
                "    name1 STRING,\n" +
                "    score BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'redis', -- ?????? connector ??? redis ?????????\n" +
                "    'hostname' = '"+parameter.get("redis.host")+"', -- redis server ip\n" +
                "    'port' = '6391', -- redis server ??????\n" +
                "    'password' = '"+parameter.get("redis.password")+"',\n" +
                "    'format' = 'json', -- ?????? format ????????????\n" +
                "    'lookup.cache.max-rows' = '500', -- guava local cache ????????????\n" +
                "    'lookup.cache.ttl' = '3600', -- guava local cache ttl\n" +
                "    'lookup.max-retries' = '1' -- redis ?????????????????????????????????\n" +
                ")";

        String joinSql = "SELECT o.f0, o.f1, c.name, c.name1, c.score\n" +
                "FROM sourceTable AS o\n" +
                "LEFT JOIN dimTable FOR SYSTEM_TIME AS OF o.proctime AS c\n" +
                "ON o.f0 = c.name";

        tableEnv.executeSql(createSql);

        tableEnv.executeSql(joinSql).print();

        env.execute();
    }

    private static class CustomDefinedSource implements SourceFunction<Row>, ResultTypeQueryable<Row> {

        private volatile boolean isCancel;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {

            while (!this.isCancel) {

                sourceContext.collect(Row.of("a", "b", 1L));

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
