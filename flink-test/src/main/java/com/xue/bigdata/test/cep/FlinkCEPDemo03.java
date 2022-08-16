package com.xue.bigdata.test.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;


/**
 * 超时未支付：下单后 10 分钟内没有支付的订单
 * @author: mingway
 * @date: 2022/1/27 5:22 下午
 */
public class FlinkCEPDemo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStream<PayEvent> source = env.fromElements(
                new PayEvent(1L, "create", 1597905234000L),
                new PayEvent(1L, "pay", 1597905235000L),
                new PayEvent(2L, "create", 1597905236000L),
                new PayEvent(2L, "pay", 1597905237000L),
                new PayEvent(3L, "create", 1597905239000L)

        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).keyBy(new KeySelector<PayEvent, Object>() {
            @Override
            public Object getKey(PayEvent value) throws Exception {
                return value.getUserId();
            }
        });

        // pattern
        Pattern<PayEvent, PayEvent> payEventPattern = Pattern.<PayEvent>begin("begin").where(new IterativeCondition<PayEvent>() {
            @Override
            public boolean filter(PayEvent payEvent, Context<PayEvent> context) throws Exception {
                return payEvent.getAction().equals("create");
            }
        }).next("next").where(new IterativeCondition<PayEvent>() {
            @Override
            public boolean filter(PayEvent payEvent, Context<PayEvent> context) throws Exception {
                return payEvent.getAction().equals("pay");
            }
        }).within(Time.minutes(10));

        // output tag
        OutputTag<PayEvent> orderTimeoutTag = new OutputTag<PayEvent>("orderTimeout"){};

        // CEP
        PatternStream<PayEvent> patternStream = CEP.pattern(source, payEventPattern);

        // outputTag,
        SingleOutputStreamOperator<PayEvent> result = patternStream.select(orderTimeoutTag, new PatternTimeoutFunction<PayEvent, PayEvent>() {
            @Override
            public PayEvent timeout(Map<String, List<PayEvent>> map, long l) throws Exception {
                return map.get("begin").get(0);
            }
        }, new PatternSelectFunction<PayEvent, PayEvent>() {
            @Override
            public PayEvent select(Map<String, List<PayEvent>> map) throws Exception {
                return map.get("next").get(0);
            }
        });

        // sideOutput
        DataStream<PayEvent> sideOutput = result.getSideOutput(orderTimeoutTag);
        sideOutput.printToErr();

        env.execute("testApp");
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<PayEvent> {
        private final long maxOutOfOrderness = 5000L;
        private long currentTimeStamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(PayEvent element, long previousElementTimestamp) {
            Long timeStamp = element.getTimeStamp();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.err.println(element.toString() + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
            return timeStamp;
        }
    }
}
