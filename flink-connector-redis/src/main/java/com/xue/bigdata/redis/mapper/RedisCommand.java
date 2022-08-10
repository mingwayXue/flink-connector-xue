package com.xue.bigdata.redis.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

/**
 * @author: mingway
 * @date: 2022/8/3 8:49 AM
 */
public enum RedisCommand {
    GET(RedisDataType.STRING),

    SET(RedisDataType.STRING),

    HGET(RedisDataType.HASH);

    private RedisDataType redisDataType;

    RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }
}
