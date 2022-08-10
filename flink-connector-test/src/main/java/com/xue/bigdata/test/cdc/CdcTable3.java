package com.xue.bigdata.test.cdc;//package com.xue.demo.cdc;
//
//import com.alibaba.fastjson.JSONObject;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
///**
// * @author: mingway
// * @date: 2022/1/6 11:14 上午
// */
//public class CdcTable3 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        MySqlSource<String> subscribeRuleSource = MySqlSource.<String>builder()
//                .hostname("10.21.4.17")
//                .port(3306)
//                .databaseList("platform")
//                .tableList("platform.sys_subscribe_user_shops")
//                .username("platform")
//                .password("platform@Heytea2021")
//                .startupOptions(StartupOptions.earliest())
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .build();
//        env.setParallelism(1);
//        SingleOutputStreamOperator<SubscribeRule> subscribeRuleStream = env.fromSource(subscribeRuleSource, WatermarkStrategy.noWatermarks(), "PLATFORM_SYS_SUBSCRIBE_SOURCE")
//                .map(new MapFunction<String, SubscribeRule>() {
//                    @Override
//                    public SubscribeRule map(String s) throws Exception {
//                        SubscribeRule rule = new SubscribeRule();
//                        JSONObject json = JSONObject.parseObject(s);
//                        rule.setOp(json.getString("op"));
//                        if ("d".equals(json.getString("op"))) {
//                            rule.setId(json.getJSONObject("before").getInteger("id"));
//                            rule.setReportId(json.getJSONObject("before").getString("report_id"));
//                            rule.setOpType(json.getJSONObject("before").getInteger("op_type"));
//                            rule.setShopId(json.getJSONObject("before").getInteger("shop_id"));
//                            rule.setUserId(json.getJSONObject("before").getString("user_id"));
//                        } else {
//                            rule.setId(json.getJSONObject("after").getInteger("id"));
//                            rule.setReportId(json.getJSONObject("after").getString("report_id"));
//                            rule.setOpType(json.getJSONObject("after").getInteger("op_type"));
//                            rule.setShopId(json.getJSONObject("after").getInteger("shop_id"));
//                            rule.setUserId(json.getJSONObject("after").getString("user_id"));
//                        }
//                        return rule;
//                    }
//                })
//                .filter(new FilterFunction<SubscribeRule>() {
//                    @Override
//                    public boolean filter(SubscribeRule rule) throws Exception {
//                        System.out.println(rule);
//                        return "7aa8db4da11c4dddb434c7027e0a3c9c".equals(rule.getReportId())
//                                && rule.getOpType() == 1
//                                && "606431821635579904".equals(rule.getUserId())
//                                && rule.getShopId() == 10100423
//                                ;
//                    }
//                });
//        subscribeRuleStream.print();
//
//        env.execute("test");
//    }
//}
