package com.xue.bigdata.test.heytea;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink 双流join 案例
 * @author: mingway
 * @date: 2021/12/20 6:16 下午
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        KafkaSourceBuilder<String> orderScoreBuilder = KafkaSource.<String>builder()
                .setBootstrapServers("172.24.19.36:9092,172.24.19.37:9092,172.24.19.38:9092")
                .setTopics("ods_db_production.order_score_record")
                .setGroupId("test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest());
        DataStreamSource<String> orderScoreSource = env.fromSource(orderScoreBuilder.build(), WatermarkStrategy.noWatermarks(), "comment");
        KafkaSourceBuilder<String> commentSourceBuilder = KafkaSource.<String>builder()
                .setBootstrapServers("172.24.19.36:9092,172.24.19.37:9092,172.24.19.38:9092")
                .setTopics("ods_db_production.order_comments")
                .setGroupId("test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest());
        DataStreamSource<String> commentSource = env.fromSource(commentSourceBuilder.build(), WatermarkStrategy.noWatermarks(), "comment");

        // transformation
        SingleOutputStreamOperator<OrderScore> mapOrderScore = orderScoreSource.map(new MapFunction<String, OrderScore>() {
            @Override
            public OrderScore map(String s) throws Exception {
                OrderScore os = new OrderScore();
                JSONObject json = JSONObject.parseObject(s);
                os.setId(json.getJSONObject("after").getInteger("id"));
                os.setOrderId(json.getJSONObject("after").getInteger("order_id"));
                os.setShopId(json.getJSONObject("after").getInteger("shop_id"));
                os.setScore(json.getJSONObject("after").getInteger("score"));
                os.setItemId(json.getJSONObject("after").getInteger("order_score_item_id"));
                os.setWeight(json.getJSONObject("after").getInteger("weight"));
                os.setCreatedAt(json.getJSONObject("after").getTimestamp("created_at").getTime());
                os.setWegihtScore(os.getScore() * os.getWeight() * 0.01);
                return os;
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderScore>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderScore>() {
                            @Override
                            public long extractTimestamp(OrderScore os, long recordTimestamp) {
                                return os.getCreatedAt();
                            }
                        })
        );
        SingleOutputStreamOperator<Comment> mapComment = commentSource.map(new MapFunction<String, Comment>() {
            @Override
            public Comment map(String s) throws Exception {
                Comment comment = new Comment();
                JSONObject json = JSONObject.parseObject(s);
                comment.setId(json.getJSONObject("after").getInteger("id"));
                comment.setOrderId(json.getJSONObject("after").getInteger("order_id"));
                comment.setShopId(json.getJSONObject("after").getInteger("shop_id"));
                comment.setComment(json.getJSONObject("after").getString("comment"));
                comment.setCreatedAt(json.getJSONObject("after").getTimestamp("created_at").getTime());
                return comment;
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Comment>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Comment>() {
                            @Override
                            public long extractTimestamp(Comment comment, long recordTimestamp) {
                                return comment.getCreatedAt();
                            }
                        })
        );

        // interval join
        SingleOutputStreamOperator<OrderScore> processStream = mapOrderScore.keyBy(OrderScore::getOrderId)
                .intervalJoin(mapComment.keyBy(Comment::getOrderId))
                .between(Time.seconds(-3), Time.seconds(3))
                .process(new ProcessJoinFunction<OrderScore, Comment, OrderScore>() {
                    @Override
                    public void processElement(OrderScore left, Comment right, Context ctx, Collector<OrderScore> out) throws Exception {
                        left.setComment(right.getComment());
                        out.collect(left);
                    }
                });

        // sum and print
        processStream
                .keyBy(OrderScore::getOrderId)
                .countWindow(5)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<OrderScore>() {
                    @Override
                    public OrderScore reduce(OrderScore os1, OrderScore os2) throws Exception {
                        os2.setWegihtScore(os1.getWegihtScore() + os2.getWegihtScore());
                        return os2;
                    }
                }).print();

        // sink
        //processStream.print();

        // execute
        env.execute("IntervalJoinDemoJob");
    }

    static class OrderScore {
        private int id;
        private int orderId;
        private int shopId;
        private int score;
        private int itemId;
        private int weight;
        private long createdAt;

        private String comment;
        private double wegihtScore;

        public double getWegihtScore() {
            return wegihtScore;
        }

        public void setWegihtScore(double wegihtScore) {
            this.wegihtScore = wegihtScore;
        }

        public OrderScore() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getOrderId() {
            return orderId;
        }

        public void setOrderId(int orderId) {
            this.orderId = orderId;
        }

        public int getShopId() {
            return shopId;
        }

        public void setShopId(int shopId) {
            this.shopId = shopId;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public int getItemId() {
            return itemId;
        }

        public void setItemId(int itemId) {
            this.itemId = itemId;
        }

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

        public long getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(long createdAt) {
            this.createdAt = createdAt;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        @Override
        public String toString() {
            return "OrderScore{" +
                    "id=" + id +
                    ", orderId=" + orderId +
                    ", shopId=" + shopId +
                    ", score=" + score +
                    ", itemId=" + itemId +
                    ", weight=" + weight +
                    ", createdAt=" + createdAt +
                    ", comment='" + comment + '\'' +
                    ", wegihtScore=" + wegihtScore +
                    '}';
        }
    }

    static class Comment {
        private int id;
        private int orderId;
        private int shopId;
        private String comment;
        private long createdAt;

        public Comment() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getOrderId() {
            return orderId;
        }

        public void setOrderId(int orderId) {
            this.orderId = orderId;
        }

        public int getShopId() {
            return shopId;
        }

        public void setShopId(int shopId) {
            this.shopId = shopId;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public long getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(long createdAt) {
            this.createdAt = createdAt;
        }
    }
}
