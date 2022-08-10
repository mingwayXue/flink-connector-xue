package com.xue.bigdata.redis.container;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * @author: mingway
 * @date: 2022/8/3 9:02 AM
 */
/**
 * The container for all available Redis commands.
 */
public interface RedisCommandsContainer extends Cloneable, Serializable {
    void open() throws Exception;

    byte[] get(byte[] key);

    List<Object> multiGet(List<byte[]> key);

    byte[] hget(byte[] key, byte[] hashField);

    void close() throws IOException;
}
