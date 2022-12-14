package com.xue.bigdata.redis.source;

import com.xue.bigdata.redis.container.RedisCommandsContainer;
import com.xue.bigdata.redis.container.RedisCommandsContainerBuilder;
import com.xue.bigdata.redis.mapper.LookupRedisMapper;
import com.xue.bigdata.redis.mapper.RedisCommand;
import com.xue.bigdata.redis.mapper.RedisCommandDescription;
import com.xue.bigdata.redis.options.RedisLookupOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author: mingway
 * @date: 2022/8/4 8:16 AM
 */
public class RedisRowDataBatchLookupFunction extends TableFunction<List<RowData>> {
    private static final Logger LOG = LoggerFactory.getLogger(
            RedisRowDataBatchLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private String additionalKey;
    private LookupRedisMapper lookupRedisMapper;
    private RedisCommand redisCommand;

    protected final RedisLookupOptions redisLookupOptions;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    private final boolean isBatchMode;

    private final int batchSize;

    private final int batchMinTriggerDelayMs;

    private transient Cache<Object, RowData> cache;

    private transient Consumer<Object[]> evaler;

    private static final byte[] DEFAULT_JSON_BYTES = "{}".getBytes();

    public RedisRowDataBatchLookupFunction(
            FlinkJedisConfigBase flinkJedisConfigBase
            , LookupRedisMapper lookupRedisMapper,
            RedisLookupOptions redisLookupOptions) {

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.lookupRedisMapper = lookupRedisMapper;
        this.redisLookupOptions = redisLookupOptions;
        RedisCommandDescription redisCommandDescription = lookupRedisMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.additionalKey = redisCommandDescription.getAdditionalKey();

        this.cacheMaxSize = this.redisLookupOptions.getCacheMaxSize();
        this.cacheExpireMs = this.redisLookupOptions.getCacheExpireMs();
        this.maxRetryTimes = this.redisLookupOptions.getMaxRetryTimes();

        this.isBatchMode = this.redisLookupOptions.isBatchMode();

        this.batchSize = this.redisLookupOptions.getBatchSize();

        this.batchMinTriggerDelayMs = this.redisLookupOptions.getBatchMinTriggerDelayMs();
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param objects the lookup key. Currently only support single rowkey.
     */
    public void eval(Object... objects) throws IOException {

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                // fetch result
                this.evaler.accept(objects);
                break;
            } catch (Exception e) {
                LOG.error(String.format("Redis lookup error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Redis lookup failed.", e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }


    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");

        try {
            this.redisCommandsContainer =
                    RedisCommandsContainerBuilder
                            .build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        this.cache = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                .recordStats()
                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                .maximumSize(cacheMaxSize)
                .build();

        if (cache != null) {
            context.getMetricGroup()
                    .gauge("lookupCacheHitRate", (Gauge<Double>) () -> cache.stats().hitRate());

            this.evaler = in -> {

                List<Object> inner = (List<Object>) in[0];
                List<byte[]> keys = inner
                        .stream()
                        .map(o -> {
                            if (o instanceof BinaryStringData) {
                                return ((BinaryStringData) o).getJavaObject().getBytes();
                            } else {
                                return String.valueOf(o).getBytes();
                            }
                        })
                        .collect(Collectors.toList());
                List<Object> value = null;
                switch (redisCommand) {
                    case GET:
                        value = this.redisCommandsContainer.multiGet(keys);
                        break;
                    default:
                        throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
                }
                List<RowData> result = value
                        .stream()
                        .map(o -> {
                            if (null == o) {
                                return this.lookupRedisMapper.deserialize(DEFAULT_JSON_BYTES);
                            } else {
                                return this.lookupRedisMapper.deserialize((byte[]) o);
                            }
                        })
                        .collect(Collectors.toList());

                collect(result);
            };

            //            this.evaler = in -> {
            //                RowData cacheRowData = cache.getIfPresent(in);
            //                if (cacheRowData != null) {
            ////                    collect(cacheRowData);
            //                } else {
            //                    // fetch result
            //                    byte[] key = lookupRedisMapper.serialize(in);
            //
            //                    byte[] value = null;
            //
            //                    switch (redisCommand) {
            //                        case GET:
            //                            value = this.redisCommandsContainer.get(key);
            //                            break;
            //                        case HGET:
            //                            value = this.redisCommandsContainer.hget(key, this.additionalKey.getBytes());
            //                            break;
            //                        default:
            //                            throw new IllegalArgumentException("Cannot process such data type: " +
            //                            redisCommand);
            //                    }
            //
            //                    RowData rowData = this.lookupRedisMapper.deserialize(value);
            //
            //                    collect(rowData);
            //
            //                    if (null != rowData) {
            //                        cache.put(key, rowData);
            //                    }
            //                }
            //            };

        } else {
            this.evaler = in -> {

                List<Object[]> inner = (List<Object[]>) in[0];

                List<byte[]> keys = inner
                        .stream()
                        .map(lookupRedisMapper::serialize)
                        .collect(Collectors.toList());

                List<Object> value = null;

                switch (redisCommand) {
                    case GET:
                        value = this.redisCommandsContainer.multiGet(keys);
                        break;
                    default:
                        throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
                }

                List<RowData> result = value
                        .stream()
                        .map(o -> this.lookupRedisMapper.deserialize((byte[]) o))
                        .collect(Collectors.toList());

                collect(result);
            };
        }

        LOG.info("end open.");
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (redisCommandsContainer != null) {
            try {
                redisCommandsContainer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("end close.");
    }
}
