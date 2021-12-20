package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.sql.Timestamp;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> StreamA = env
                .fromElements(
                        Tuple3.of("user-1", "order", (12 * 60 + 10) * 60 * 1000L),
                        Tuple3.of("user-1", "order", 13 * 60 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                                return element.f2;
                                            }
                                        }
                                )
                );

        DataStream<Tuple3<String, String, Long>> StreamB = env
                .fromElements(
                        Tuple3.of("user-1", "click", (11 * 60 + 20) * 60 * 1000L),
                        Tuple3.of("user-1", "click", (12 * 60 + 15) * 60 * 1000L),
                        Tuple3.of("user-1", "click", (13 * 60 + 25) * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                                return element.f2;
                                            }
                                        }
                                )
                );

        // 两条流都要进行keyBy
        StreamA.keyBy(r -> r.f0)
                .intervalJoin(StreamB.keyBy(r -> r.f0))
                // 和过去一小时以及未来十五分钟的数据进行join
                .between(Time.hours(-1), Time.minutes(15))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        String l = left.f0 + ", " + left.f1 + ", " + new Timestamp(left.f2);
                        String r = right.f0 + ", " + right.f1 + ", " + new Timestamp(right.f2);
                        out.collect(l + " => " + r);
                    }
                })
                .print();

        env.execute();
    }
}
