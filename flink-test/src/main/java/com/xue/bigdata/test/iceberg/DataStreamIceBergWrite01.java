package com.xue.bigdata.test.iceberg;

import com.xue.bigdata.test.connector.RedisLookupTest;
import com.xue.bigdata.test.util.CustomDefinedSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.Map;

public class DataStreamIceBergWrite01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new org.apache.flink.configuration.Configuration());
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///workspace/github/flink-connector-xue/flink-test/src/main/resources");
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        String HIVE_CATALOG = "iceberg";
        Configuration hadoopConf = new Configuration();
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
        // Catalog hiveCatalog = catalogLoader.loadCatalog();
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("ice_db", "iceberg_002"));

        // custom source
        DataStreamSource<Row> source = env.addSource(new CustomDefinedSource());

        SingleOutputStreamOperator<RowData> dataStream = source.map(new MapFunction<Row, RowData>() {
            @Override
            public RowData map(Row row) throws Exception {
                GenericRowData rowData = new GenericRowData(3);
                rowData.setField(0, row.getField(0));
                rowData.setField(1, StringData.fromString(String.valueOf(row.getField(1))));
                rowData.setField(2, StringData.fromString(String.valueOf(row.getField(2))));
                return rowData;
            }
        });

        FlinkSink.forRowData(dataStream)
                // .table(table) // 这个 .table 也可以不写，指定tableLoader 对应的路径就可以
                .tableLoader(tableLoader)
                .writeParallelism(1)
                // overwrite 默认为false,追加数据。如果设置为true 就是覆盖数据
                .overwrite(false)
                .append();
        env.execute("Test");
    }
}
