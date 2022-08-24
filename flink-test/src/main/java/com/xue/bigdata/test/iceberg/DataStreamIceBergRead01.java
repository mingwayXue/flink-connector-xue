package com.xue.bigdata.test.iceberg;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import java.util.HashMap;
import java.util.Map;

public class DataStreamIceBergRead01 {
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
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("ice_db", "iceberg_001"));

        DataStream<RowData> dataStream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(false)
                .build();


        dataStream.printToErr();

        env.execute("Test01");
    }
}
