package com.xue.bigdata.redis.sink;

import com.xue.bigdata.redis.mapper.SetRedisMapper;
import com.xue.bigdata.redis.options.RedisWriteOptions;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * @author: mingway
 * @date: 2022/8/8 10:38 PM
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    /**
     * Data type to configure the formats.
     */
    protected final DataType physicalDataType;

    protected final RedisWriteOptions redisWriteOptions;

    public RedisDynamicTableSink(
            DataType physicalDataType
            , RedisWriteOptions redisWriteOptions) {

        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.redisWriteOptions = redisWriteOptions;
    }

    private @Nullable
    SerializationSchema<RowData> createSerialization(
            Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // UPSET mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();

        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }

        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FlinkJedisConfigBase flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
                .setHost(this.redisWriteOptions.getHostname())
                .setPort(this.redisWriteOptions.getPort())
                .setPassword(this.redisWriteOptions.getPassword())
                .build();

        RedisMapper<RowData> redisMapper = null;

        switch (this.redisWriteOptions.getWriteMode()) {
            case "string":
                redisMapper = new SetRedisMapper();
                break;
            default:
                throw new RuntimeException("其他类型 write mode 请自定义实现");
        }

        return SinkFunctionProvider.of(new RedisSink<>(
                flinkJedisConfigBase
                , redisMapper));
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "redis";
    }
}
