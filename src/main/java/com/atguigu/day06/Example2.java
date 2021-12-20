package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .socketTextStream("hadoop101", 9999)
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            // 将迟到元素发送到侧输出流中去
                            // output方法第一个参数是：侧输出标签，也就是侧输出流的名字
                            // 第二个参数是将要发送到侧输出流的数据
                            ctx.output(
                                    // 匿名类作为参数，泛型是侧输出流中的元素类型
                                    // 侧输出标签也是一个单例
                                    new OutputTag<String>("late-element"){},
                                    "迟到元素到达！" + value + "，当前水位线是：" + "" +
                                            ctx.timerService().currentWatermark()
                            );
                        } else {
                            out.collect("正常到达的数据：" + value + "当前水位线是：" +
                                    ctx.timerService().currentWatermark());
                        }
                    }
                });

        result.print("主流输出");
        // 这里的侧输出标签一定要和上面使用过的侧输出标签一样，因为是单例，只会被实例化一次
        result.getSideOutput(new OutputTag<String>("late-element"){}).print("侧输出流");

        env.execute();
    }
}
