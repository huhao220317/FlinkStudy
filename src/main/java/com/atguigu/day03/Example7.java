package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// KeyedProcessFunction 只能用在 keyBy 之后
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据：" + in + " 到达了！到达机器的处理时间是：" + new Timestamp(ctx.timerService().currentProcessingTime()));
                        // 注册一个当前机器时间10秒钟之后的定时事件
                        long tenSecLater = ctx.timerService().currentProcessingTime() + 10 * 1000L;
                        out.collect("注册了一个时间戳是：" + new Timestamp(tenSecLater) + " 的定时器");
                        ctx.timerService().registerProcessingTimeTimer(tenSecLater);
                    }

                    // timestamp 参数是：tenSecLater
                    // 当时间到达 timestamp 时，触发onTimer的执行
                    // 每个 key 都会维护自己的定时器，所以定时器也是process算子的内部状态
                    // 针对每个 key，在某一个时间戳，只能注册一个定时器，如果注册多个，后面的注册行为不起作用
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了！触发时间是：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
