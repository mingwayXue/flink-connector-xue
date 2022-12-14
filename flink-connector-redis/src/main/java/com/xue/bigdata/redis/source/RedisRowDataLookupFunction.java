package com.xue.bigdata.redis.source;

/**
 * @author: mingway
 * @date: 2022/8/3 8:58 AM
 */

import com.xue.bigdata.redis.container.RedisCommandsContainer;
import com.xue.bigdata.redis.container.RedisCommandsContainerBuilder;
import com.xue.bigdata.redis.mapper.LookupRedisMapper;
import com.xue.bigdata.redis.mapper.RedisCommand;
import com.xue.bigdata.redis.mapper.RedisCommandDescription;
import com.xue.bigdata.redis.options.RedisLookupOptions;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The RedisRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 */
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(
            RedisRowDataLookupFunction.class);
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

    public RedisRowDataLookupFunction(
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
                LOG.error(String.format("HBase lookup error, retry times = %d", retry), e);
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
                RowData cacheRowData = cache.getIfPresent(in);
                if (cacheRowData != null) {
//                    collect(cacheRowData);
                } else {
                    // fetch result
                    byte[] key = lookupRedisMapper.serialize(in);

                    byte[] value = null;

                    switch (redisCommand) {
                        case GET:
                            value = this.redisCommandsContainer.get(key);
                            break;
                        case HGET:
                            value = this.redisCommandsContainer.hget(key, this.additionalKey.getBytes());
                            break;
                        default:
                            throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
                    }

                    RowData rowData = this.lookupRedisMapper.deserialize(value);

                    collect(rowData);

                    if (null != rowData) {
                        cache.put(key, rowData);
                    }
                }
            };

        } else {
            this.evaler = in -> {
                // fetch result
                byte[] key = lookupRedisMapper.serialize(in);

                byte[] value = null;

                switch (redisCommand) {
                    case GET:
                        value = this.redisCommandsContainer.get(key);
                        break;
                    case HGET:
                        value = this.redisCommandsContainer.hget(key, this.additionalKey.getBytes());
                        break;
                    default:
                        throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
                }

                RowData rowData = this.lookupRedisMapper.deserialize(value);

                collect(rowData);
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
