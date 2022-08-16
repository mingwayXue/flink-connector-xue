package com.xue.bigdata.test.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * 连续登录场景：连续2次登录失败
 * @author: mingway
 * @date: 2022/1/27 4:42 下午
 */
public class FlinkCEPDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStream<LogInEvent> source = env.fromElements(
                new LogInEvent(1L, "fail", 1597905234000L),
                new LogInEvent(1L, "success", 1597905235000L),
                new LogInEvent(2L, "fail", 1597905236000L),
                new LogInEvent(2L, "fail", 1597905237000L),
                new LogInEvent(2L, "fail", 1597905238000L),
                new LogInEvent(3L, "fail", 1597905239000L),
                new LogInEvent(3L, "success", 1597905240000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).keyBy(new KeySelector<LogInEvent, Object>() {
            @Override
            public Object getKey(LogInEvent value) throws Exception {
                return value.getUserId();
            }
        });

        // Pattern
        /*Pattern<LogInEvent, LogInEvent> logInEventPattern = Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent logInEvent, Context<LogInEvent> context) throws Exception {
                return logInEvent.getIsSuccess().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent logInEvent, Context<LogInEvent> context) throws Exception {
                return logInEvent.getIsSuccess().equals("fail");
            }
        }).within(Time.seconds(5));*/
        Pattern<LogInEvent, LogInEvent> logInEventPattern = Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent logInEvent, Context<LogInEvent> context) throws Exception {
                return logInEvent.getIsSuccess().equals("fail");
            }
        }).times(2).within(Time.seconds(5));

        // CEP
        PatternStream<LogInEvent> patternStream = CEP.pattern(source, logInEventPattern);

        SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<LogInEvent, AlertEvent>() {
            @Override
            public void processMatch(Map<String, List<LogInEvent>> map, Context context, Collector<AlertEvent> collector) throws Exception {
                List<LogInEvent> start = map.get("start");
                List<LogInEvent> next = map.get("next");
                System.err.println("start:" + start + ",next:" + next);

                collector.collect(new AlertEvent(String.valueOf(start.get(0).getUserId()), "出现连续登陆失败"));
            }
        });

        process.printToErr();

        env.execute("testApp");
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<LogInEvent> {
        private final long maxOutOfOrderness = 5000L;
        private long currentTimeStamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(LogInEvent element, long previousElementTimestamp) {
            Long timeStamp = element.getTimeStamp();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.err.println(element.toString() + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
            return timeStamp;
        }
    }

}
