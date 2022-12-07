package com.xue.bigdata.test.source;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xue.bigdata.test.source.hybrid.JDBCSource;
import com.xue.bigdata.test.source.hybrid.TableSelect;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class HybridSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        List<TableSelect> sqlList = new ArrayList<>();
        TableSelect ts1 = new TableSelect();
        ts1.setDb("ads");
        ts1.setTable("api_audit_supervision_risk_push_log");
        ts1.setSelectSql("select id,is_success,into_sys_time from ads.api_audit_supervision_risk_push_log limit 20");
        sqlList.add(ts1);
        JDBCSource jdbcSource = JDBCSource.builder()
                .url("url")
                .driver("com.mysql.cj.jdbc.Driver")
                .username("user")
                .password("name")
                .sqlList(sqlList)
                .build();

        /*DataStreamSource<String> jdbcSourceStream = env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "JDBC_SOURCE");

        jdbcSourceStream.printToErr();*/

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("host")
                .setTopics("ods_db_production__orders")
                .setGroupId("test-xue")
                // .setStartingOffsets(OffsetsInitializer.latest())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setClientIdPrefix("flink_")
                .setProperty("partition.discovery.interval.ms", "10000")
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "1000")
                .build();


        HybridSource<String> hybridSource = HybridSource.builder(jdbcSource).addSource(kafkaSource).build();

        DataStreamSource<String> hybridSourceStream = env.fromSource(hybridSource, WatermarkStrategy.noWatermarks(), "HYBRID_SOURCE", TypeInformation.of(String.class));

        hybridSourceStream.printToErr();

        env.execute("HybridSourceDemo");
    }
}
