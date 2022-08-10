package com.xue.bigdata.test.cep;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 创建订单之后15分钟之内一定要付款，否则就取消订单。
 * 订单状态说明：
 * 1 创建订单,等待支付
 * 2 支付订单完成
 * 3 取消订单，申请退款
 * 4 已发货
 * 5 确认收货，已经完成
 * @author: mingway
 * @date: 2022/1/27 6:11 下午
 */
public class CheckOrderTimeoutWithCep {

    private static FastDateFormat fastDateFormat =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStreamSource<String> source = env.fromElements(
                "20160728001511050311389390,1,2016-07-28 00:15:11,295",
                "20160801000227050311955990,1,2016-07-28 00:16:12,165",
                "20160728001511050311389390,2,2016-07-28 00:18:11,295",
                "20160801000227050311955990,2,2016-07-28 00:18:12,165",
                "20160728001511050311389390,3,2016-07-29 08:06:11,295",
                "20160801000227050311955990,4,2016-07-29 12:21:12,165",
                "20160804114043050311618457,1,2016-07-30 00:16:15,132",
                "20160801000227050311955990,5,2016-07-30 18:13:24,165"
        );

        KeyedStream<OrderDetail, String> stream = source.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                String[] strings = value.split(",");
                return new OrderDetail(strings[0], strings[1], strings[2],
                        Double.parseDouble(strings[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                long extractTimestamp = 0;
                try {
                    extractTimestamp = fastDateFormat.parse(element.orderCreateTime).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return extractTimestamp;
            }
        })).keyBy(x -> x.orderId);

        // pattern
        Pattern<OrderDetail, OrderDetail> pattern = Pattern.<OrderDetail>begin("start").where(new SimpleCondition<OrderDetail>() {
            @Override
            public boolean filter(OrderDetail value) throws Exception {
                return value.status.equals("1");
            }
        }).followedBy("second").where(new SimpleCondition<OrderDetail>() {
            @Override
            public boolean filter(OrderDetail value) throws Exception {
                return value.status.equals("2");
            }
        }).within(Time.minutes(15));

        // CEP
        PatternStream<OrderDetail> patternStream = CEP.pattern(stream, pattern).inEventTime();

        // outPutTag
        OutputTag outputTag = new OutputTag("timeout", TypeInformation.of(OrderDetail.class));

        SingleOutputStreamOperator result = patternStream.select(outputTag, new PatternTimeoutFunction<OrderDetail, OrderDetail>() {
            @Override
            public OrderDetail timeout(Map<String, List<OrderDetail>> map, long l) throws Exception {
                List<OrderDetail> start = map.get("start");
                return start.iterator().next();
            }
        }, new PatternSelectFunction<OrderDetail, OrderDetail>() {
            @Override
            public OrderDetail select(Map<String, List<OrderDetail>> map) throws Exception {
                List<OrderDetail> second = map.get("second");
                return second.iterator().next();
            }
        });

        result.printToErr("支付成功的订单:");

        result.getSideOutput(outputTag).printToErr("超时的订单:");

        env.execute("testApp");
    }

    public static class OrderDetail{
        //订单编号
        public String orderId;
        //订单状态
        public String status;
        //下单时间
        public String orderCreateTime;
        //订单金额
        public Double price;
        //无参构造必须带上
        public OrderDetail() {
        }
        public OrderDetail(String orderId, String status, String orderCreateTime,
                           Double price) {
            this.orderId = orderId;
            this.status = status;
            this.orderCreateTime = orderCreateTime;
            this.price = price;
        }
        @Override
        public String toString() {
            return orderId+"\t"+status+"\t"+orderCreateTime+"\t"+price;
        }
    }
}
