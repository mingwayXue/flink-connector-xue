/*
package com.xue.bigdata.test.protobuf;

import com.google.common.collect.ImmutableMap;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

*/
/**
 * @author: mingway
 * @date: 2022/8/16 8:48 AM
 *//*

public class SocketWriteTest {
    public static void main(String[] args) throws Exception {

        ServerSocket serversocket = new ServerSocket(9999);

        final Socket socket = serversocket.accept();

        int i = 0;

        while (true) {

            Map<String, Integer> map = ImmutableMap.of("key1", 1, "地图", i);

            Test test = Test.newBuilder()
                    .setName("姓名" + i)
                    .addNames("姓名列表" + i)
                    .putAllSiMap(map)
                    .build();

            System.out.println(JacksonUtils.bean2Json(test));
            byte[] b = test.toByteArray();

            socket.getOutputStream().write(b);

            socket.getOutputStream().flush();
            i++;

            if (i == 10) {
                break;
            }

            Thread.sleep(500);
        }

        socket.close();
        serversocket.close();

    }
}
*/
