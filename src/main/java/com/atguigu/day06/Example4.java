package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .socketTextStream("localhost", 9999)
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        // 设置最大延迟时间是5秒钟
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                // 窗口长度也是5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 侧输出流中的元素的泛型和数据流中的元素的泛型必须一样
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late"){})
                // 窗口会等待5秒钟的迟到事件
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        // 这个状态变量的作用域是当前窗口，只有当前窗口可以读写这个状态变量
                        // 用来记录是否是窗口的第一次触发
                        ValueState<Boolean> isFirstTrigger = context.windowState().getState(
                                new ValueStateDescriptor<Boolean>("is-first-trigger", Types.BOOLEAN)
                        );
                        if (isFirstTrigger.value() == null) {
                            // 当窗口第一次触发计算时，标志位状态变量中的值是null
                            out.collect(
                                    "窗口第一次触发计算！" + context.window().getStart() + "~" + context.window().getEnd() + "" +
                                            "窗口中共有：" + elements.spliterator().getExactSizeIfKnown()
                            );
                            // 将标志位设置为true
                            isFirstTrigger.update(true);
                        } else if (isFirstTrigger.value() != null && isFirstTrigger.value()) {
                            out.collect(
                                    "迟到元素到达！窗口继续触发计算！" + context.window().getStart() + "~" + context.window().getEnd() + "" +
                                            "窗口中共有：" + elements.spliterator().getExactSizeIfKnown()
                            );
                        }
                        // `elements.spliterator().getExactSizeIfKnown()`的意思是获取迭代器中的元素数量
                    }
                });

        result.print("主流输出");

        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late"){}).print("侧输出流");

        env.execute();
    }
}
