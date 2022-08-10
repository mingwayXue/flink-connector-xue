package com.xue.bigdata.test.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 检测IP是否变更
 * @author: mingway
 * @date: 2022/1/27 5:49 下午
 */
public class CheckIPChangeWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStreamSource<String> source = env.fromElements(
                "192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45",
                "192.168.54.172,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:23:46",
                "192.168.145.77,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:23:47",
                "192.168.52.100,zhubajie,https://icbc.com.cn/transfer.html,2020-02-12 12:23:47",
                "192.168.54.172,tangseng,https://icbc.com.cn/transfer.html,2020-02-12 12:23:48",
                "192.168.145.77,sunwukong,https://icbc.com.cn/transfer.html,2020-02-12 12:23:49",
                "192.168.145.77,sunwukong,https://icbc.com.cn/save.html,2020-02-12 12:23:52",
                "192.168.52.100,zhubajie,https://icbc.com.cn/save.html,2020-02-12 12:23:53",
                "192.168.54.172,tangseng,https://icbc.com.cn/save.html,2020-02-12 12:23:54",
                "192.168.54.172,tangseng,https://icbc.com.cn/buy.html,2020-02-12 12:23:57",
                "192.168.145.77,sunwukong,https://icbc.com.cn/buy.html,2020-02-12 12:23:58",
                "192.168.52.101,zhubajie,https://icbc.com.cn/buy.html,2020-02-12 12:23:59"
        );

        KeyedStream<LoginInfo, String> keyedStream = source.map(new MapFunction<String, LoginInfo>() {
            @Override
            public LoginInfo map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginInfo(split[0], split[1], split[2], split[3]);
            }
        }).keyBy(x -> x.username);

        // pattern
        Pattern<LoginInfo, LoginInfo> loginInfoPattern = Pattern.<LoginInfo>begin("begin").where(new SimpleCondition<LoginInfo>() {
            @Override
            public boolean filter(LoginInfo value) throws Exception {
                return value.username != null;
            }
        }).next("next").where(new IterativeCondition<LoginInfo>() {
            @Override
            public boolean filter(LoginInfo loginInfo, Context<LoginInfo> context) throws Exception {
                Iterable<LoginInfo> begin = context.getEventsForPattern("begin");
                Iterator<LoginInfo> iterator = begin.iterator();

                while (iterator.hasNext()) {
                    LoginInfo userLogin = iterator.next();
                    if (!loginInfo.ip.equals(userLogin.ip)) {
                        return true;
                    }
                }

                return false;
            }
        });

        PatternStream<LoginInfo> patternStream = CEP.pattern(keyedStream, loginInfoPattern).inProcessingTime();

        patternStream.select(new PatternSelectFunction<LoginInfo, LoginInfo>() {
            @Override
            public LoginInfo select(Map<String, List<LoginInfo>> map) throws Exception {
                List<LoginInfo> begin = map.get("begin");
                List<LoginInfo> next = map.get("next");

                LoginInfo beginData = begin.iterator().next();
                LoginInfo nextData = next.iterator().next();
                System.err.println("满足begin模式中的数据 ：" + beginData);
                System.err.println("满足next模式中的数据：" + nextData);

                return nextData;
            }
        });

        env.execute("testApp");
    }

    public static class LoginInfo {
        public String ip;
        public String username;
        public String operateUrl;
        public String time;
        //无参构造必须带上
        public LoginInfo() {}
        public LoginInfo(String ip, String username, String operateUrl, String
                time) {
            this.ip = ip;
            this.username = username;
            this.operateUrl = operateUrl;
            this.time = time;
        }
        @Override
        public String toString() {
            return ip+"\t"+username+"\t"+operateUrl+"\t"+time;
        }
    }
}
