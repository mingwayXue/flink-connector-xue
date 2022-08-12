package com.xue.bigdata.test.sql;

import com.xue.bigdata.test.udf.Mod_UDF;
import com.xue.bigdata.test.udf.StatusMapper_UDF;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class ParserTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple3<String, Long, Long>> tuple3DataStream =
                env.fromCollection(Arrays.asList(
                                Tuple3.of("2", 1L, 1627254000000L),
                                Tuple3.of("2", 1L, 1627218000000L + 5000L),
                                Tuple3.of("2", 101L, 1627218000000L + 6000L),
                                Tuple3.of("2", 201L, 1627218000000L + 7000L),
                                Tuple3.of("2", 301L, 1627218000000L + 7000L),
                                Tuple3.of("2", 301L, 1627218000000L + 7000L),
                                Tuple3.of("2", 301L, 1627218000000L + 7000L),
                                Tuple3.of("2", 301L, 1627218000000L + 7000L),
                                Tuple3.of("2", 301L, 1627218000000L + 7000L),
                                Tuple3.of("2", 301L, 1627218000000L + 86400000 + 7000L)))
                        .assignTimestampsAndWatermarks(
                                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Long>>(Time.seconds(0L)) {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, Long> element) {
                                        return element.f2;
                                    }
                                });
        tableEnv.createTemporarySystemFunction("mod", new Mod_UDF());
        tableEnv.createTemporarySystemFunction("status_mapper", new StatusMapper_UDF());
        tableEnv.createTemporaryView("source_db.source_table", tuple3DataStream,
                "status, id, timestamp, rowtime.rowtime");

        String sql = "SELECT\n"
                + "  count(1),\n"
                + "  cast(tumble_start(rowtime, INTERVAL '1' DAY) as string)\n"
                + "FROM\n"
                + "  source_db.source_table\n"
                + "GROUP BY\n"
                + "  tumble(rowtime, INTERVAL '1' DAY)";

        Table result = tableEnv.sqlQuery(sql);

        tableEnv.toChangelogStream(result).printToErr();

        env.execute();
    }
}
