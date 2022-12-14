package com.xue.bigdata.redis.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.data.RowData;

/**
 * @author: mingway
 * @date: 2022/8/8 10:43 PM
 */
public class SetRedisMapper implements RedisMapper<RowData> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(RowData data) {
        return data.getString(0).toString();
    }

    @Override
    public String getValueFromData(RowData data) {
        return data.getString(1).toString();
    }
}
