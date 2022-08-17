package com.xue.bigdata.test.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Deduplication 是去重，row_number = 1 的场景。
 * 有一点不一样在于其排序字段一定是时间属性列，不能是其他非时间属性的普通列。（如果不是时间属性列，则会翻译成 TopN 算子）
 * Deduplication 相比 TopN 算子专门做了对应的优化，性能会有很大提升。
 */
public class DeduplicationTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE source_table (\n" +
                "    user_id BIGINT COMMENT '用户 id',\n" +
                "    level STRING COMMENT '用户等级',\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)) COMMENT '事件时间戳',\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.level.length' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '1000000'\n" +
                ")";
        tableEnv.executeSql(sql);

        String sinkSql = "CREATE TABLE sink_table (\n" +
                "    level STRING COMMENT '等级',\n" +
                "    uv BIGINT COMMENT '当前等级用户数',\n" +
                "    row_time timestamp(3) COMMENT '时间戳'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        /*
insert into sink_table
select
    level,
    ,count(1) as uv
    ,max(row_time) as row_time
from (
    select user_id,level,row_time,row_number() over(partition by user_id order by row_time) as rn
    from source_table
) where rn = 1
group by level
         */
        String execSql = "insert into sink_table\n" +
                "select \n" +
                "    level\n" +
                "    ,count(1) as uv\n" +
                "    ,max(row_time) as row_time\n" +
                "from (\n" +
                "    select user_id,level,row_time,row_number() over(partition by user_id order by row_time) as rn\n" +
                "    from source_table\n" +
                ") where rn = 1\n" +
                "group by level";
        tableEnv.executeSql(execSql);

    }
}
