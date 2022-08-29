package com.xue.bigdata.test.iceberg;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;

import java.util.HashMap;
import java.util.Map;

/**
 * iceberg 表，合并小文件 + 快照删除
 * @author: mingway
 * @date: 2022/8/27 11:18 AM
 */
public class TableCompaction01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new org.apache.flink.configuration.Configuration());
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///xue/workspace/github/flink-connector-xue/flink-test/src/main/resources");
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
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
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("ice_db", "iceberg_001"));
        tableLoader.open();
        Table table = tableLoader.loadTable();

        // 合并小文件
        Actions.forTable(env, table)
                .rewriteDataFiles()
                .maxParallelism(1)
                .targetSizeInBytes(128 * 1024 * 1024)
                .execute();

        // 清除历史快照
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
            table.expireSnapshots().expireOlderThan(snapshot.timestampMillis()).commit();
        }
    }
}
