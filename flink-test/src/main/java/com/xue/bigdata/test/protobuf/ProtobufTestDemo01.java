package com.xue.bigdata.test.protobuf;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.xue.bigdata.protobuf.ProtobufUtils;

import java.util.Map;

public class ProtobufTestDemo01 {
    public static void main(String[] args) throws Exception {
        Map<String, Integer> map = ImmutableMap.of("key1", 1, "地图", 1);
        Test test = Test.newBuilder()
                .setName("姓名")
                .addNames("姓名列表")
                .putAllSiMap(map)
                .build();
        System.out.println("## 序列化");
        System.out.println(JsonFormat.printToString(test));
        byte[] b = test.toByteArray();

        System.out.println("## 反序列化");
        Test defaultInstance = ProtobufUtils.getDefaultInstance(Test.class);
        Message message = defaultInstance
                .newBuilderForType()
                .mergeFrom(b)
                .build();
        System.out.println(message);

    }
}
