package com.xue.bigdata.redis;

import com.xue.bigdata.redis.options.RedisLookupOptions;
import com.xue.bigdata.redis.options.RedisOptions;
import com.xue.bigdata.redis.options.RedisWriteOptions;
import com.xue.bigdata.redis.sink.RedisDynamicTableSink;
import com.xue.bigdata.redis.source.RedisDynamicTableSource;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.xue.bigdata.redis.options.RedisOptions.*;
import static com.xue.bigdata.redis.options.RedisWriteOptions.*;

/**
 * @author: mingway
 * @date: 2022/8/2 11:15 PM
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    /**
     * 创建 DynamicTableSource，此时创建的自定义的 DynamicTableSource
     * @param context
     * @return
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final RedisLookupOptions redisLookupOptions = RedisOptions.getRedisLookupOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();

        Configuration c = (Configuration) context.getConfiguration();

        boolean isDimBatchMode = c.getBoolean("is.dim.batch.mode", false);

        return new RedisDynamicTableSource(
                schema.toPhysicalRowDataType()
                , decodingFormat
                , redisLookupOptions
                , isDimBatchMode);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
//        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
//                SerializationFormatFactory.class,
//                FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();

        final RedisWriteOptions redisWriteOptions = RedisOptions.getRedisWriteOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();

        return new RedisDynamicTableSink(schema.toPhysicalRowDataType()
                , redisWriteOptions);
    }

    /**
     * factory 唯一标示
     * @return
     */
    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    /**
     * 必选参数
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        return options;
    }

    /**
     * 可选参数
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(WRITE_MODE);
        options.add(IS_BATCH_MODE);
        options.add(BATCH_SIZE);
        options.add(PASSWORD);
        return options;
    }
}
