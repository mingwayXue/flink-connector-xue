package com.xue.bigdata.test.iceberg.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MySQL2IcebergWithDataStream01 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(MySQL2IcebergWithSQL01.class.getClassLoader().getResourceAsStream("application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///workspace/github/flink-connector-xue/flink-test/src/main/resources");
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStreamSource<String> mysqlSource = getMysqlSource(parameter, env);

        SingleOutputStreamOperator<RowData> rowDataStream = transfromRowData(mysqlSource);

        // rowDataStream.printToErr();
        // sink
        String HIVE_CATALOG = "iceberg";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConf.setBoolean("dfs.datanode.use.datanode.hostname", true);
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("uri", "thrift://172.24.19.27:9083");
        props.put("clients", "5");
        props.put("property-version", "1");
        props.put("catalog-type", "hive");
        props.put("warehouse", "hdfs://172.24.19.27:8020/user/hive/warehouse");
        CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, hadoopConf, props);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("ice_db", "fc01"));

        // upsert 方式插入
        FlinkSink.forRowData(rowDataStream)
                // .table(hiveCatalogTable)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                // upsert需要指定equality field columns。id不存在则插入，id存在则更新
                .upsert(true)
                // overwrite不能同时和upsert使用。每一次commit会进行partition级别的覆盖
                .overwrite(false)
                .append();

        env.execute("MySQL2IcebergWithDataStream01");
    }

    /**
     * mysql source
     * @param parameter
     * @param env
     * @return
     */
    private static DataStreamSource<String> getMysqlSource(ParameterTool parameter, StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(parameter.get("db.host"))
                .port(3306)
                .databaseList("platform")
                .tableList("platform.fc01")
                .username(parameter.get("db.username"))
                .password(parameter.get("db.password"))
                .scanNewlyAddedTableEnabled(true)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .includeSchemaChanges(false)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "SOURCE");
    }

    /**
     * 数据转换
     * @param source
     * @return
     */
    private static SingleOutputStreamOperator<RowData> transfromRowData(DataStreamSource<String> source) {
        SingleOutputStreamOperator<RowData> rowDataResult = source.flatMap(new FlatMapFunction<String, RowData>() {
            @Override
            public void flatMap(String s, Collector<RowData> out) throws Exception {
                JSONObject json = JSONObject.parseObject(s);
                String op = json.getString("op");

                String beforeOrAfter = "";
                String rowKindStr = "";
                if ("r".equals(op) || "c".equals(op)) {
                    beforeOrAfter = "after";
                    rowKindStr = "INSERT";
                } else if ("u".equals(op)) {
                    beforeOrAfter = "after";
                    rowKindStr = "UPDATE_AFTER";
                } else if ("d".equals(op)) {
                    beforeOrAfter = "before";
                    rowKindStr = "DELETE";
                }

                if (beforeOrAfter != "") {
                    JSONObject obj = json.getJSONObject(beforeOrAfter);
                    int id = obj.getIntValue("id");
                    String name = obj.getString("name");
                    GenericRowData rowData = new GenericRowData(2);
                    rowData.setRowKind(RowKind.valueOf(rowKindStr));
                    rowData.setField(0, id);
                    rowData.setField(1, StringData.fromString(name));

                    out.collect(rowData);
                }
            }
        });

        return rowDataResult;
    }
}
