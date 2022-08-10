package com.xue.bigdata.test;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: mingway
 * @date: 2022/8/4 10:55 PM
 */
public class Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("127.0.0.1", 9999);

        stringDataStreamSource.print();

        env.execute("Test01");
    }
}
