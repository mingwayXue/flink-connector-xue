package com.xue.bigdata.test.source;

import com.alibaba.fastjson.JSONObject;
import com.xue.bigdata.test.source.hybrid.JDBCSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class HybridSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        List<String> sqlList = new ArrayList<>();
        sqlList.add("");
        JDBCSource source = JDBCSource.builder()
                .port(3306)
                .hostname("")
                .username("")
                .password("")
                .fetchSize(100)
                .sqlList(sqlList)
                .build();

        DataStreamSource<String> jdbcSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "JDBC_SOURCE");

        jdbcSource.printToErr();

        // HybridSource<JSONObject> hybridSource = HybridSource.builder().addSource().build();

        env.execute("HybridSourceDemo");
    }
}
