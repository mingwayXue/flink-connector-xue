package com.xue.bigdata.test.oss;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OssTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("172.24.19.27", 9999);

        // stringDataStreamSource.print();

        stringDataStreamSource.writeAsText("oss://heytea-bigdata-oss/test/OssTest01.txt");

        env.execute("OssTest01");

    }
}
