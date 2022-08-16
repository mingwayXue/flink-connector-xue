package com.xue.bigdata.test.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
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
 * 交易活跃用户：24 小时内至少 5 次有效交易的账户
 *
 * @author: mingway
 * @date: 2022/1/27 5:11 下午
 */
public class FlinkCEPDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStream<TransactionEvent> source = env.fromElements(
                new TransactionEvent("100XX", 0.0D, 1597905234000L),
                new TransactionEvent("100XX", 100.0D, 1597905235000L),
                new TransactionEvent("100XX", 200.0D, 1597905236000L),
                new TransactionEvent("100XX", 300.0D, 1597905237000L),
                new TransactionEvent("100XX", 400.0D, 1597905238000L),
                new TransactionEvent("100XX", 500.0D, 1597905239000L),
                new TransactionEvent("101XX", 0.0D, 1597905240000L),
                new TransactionEvent("101XX", 100.0D, 1597905241000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).keyBy(new KeySelector<TransactionEvent, Object>() {
            @Override
            public Object getKey(TransactionEvent value) throws Exception {
                return value.getAccout();
            }
        });

        // pattern
        Pattern<TransactionEvent, TransactionEvent> pattern = Pattern.<TransactionEvent>begin("start").where(new SimpleCondition<TransactionEvent>() {
            @Override
            public boolean filter(TransactionEvent value) throws Exception {
                return value.getAmount() > 0;
            }
        }).timesOrMore(5).within(Time.hours(24));

        // CEP
        PatternStream<TransactionEvent> patternStream = CEP.pattern(source, pattern);

        SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<TransactionEvent, AlertEvent>() {
            @Override
            public void processMatch(Map<String, List<TransactionEvent>> map, Context context, Collector<AlertEvent> collector) throws Exception {
                List<TransactionEvent> start = map.get("start");
                List<TransactionEvent> next = map.get("next");
                System.err.println("start:" + start + ",next:" + next);
                collector.collect(new AlertEvent(start.get(0).getAccout(), "连续有效交易！"));
            }
        });

        process.printToErr();

        env.execute("testApp");
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<TransactionEvent> {
        private final long maxOutOfOrderness = 5000L;
        private long currentTimeStamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(TransactionEvent element, long previousElementTimestamp) {
            Long timeStamp = element.getTimeStamp();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.err.println(element.toString() + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
            return timeStamp;
        }
    }
}
