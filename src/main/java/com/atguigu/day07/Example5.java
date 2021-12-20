package com.atguigu.day07;

import com.atguigu.day02.Example2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// 触发器
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Example2.Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Example2.Event>() {
                            @Override
                            public long extractTimestamp(Example2.Event element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .filter(r -> r.user.equals("Mary"))
                .keyBy(r -> r.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .process(new ProcessWindowFunction<Example2.Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Example2.Event> elements, Collector<String> out) throws Exception {
                        String windowStart = new Timestamp(context.window().getStart()).toString();
                        String windowEnd = new Timestamp(context.window().getEnd()).toString();
                        long count = elements.spliterator().getExactSizeIfKnown();
                        out.collect(
                                "用户" + s + "在窗口：" +
                                        "" + windowStart + "~" + windowEnd + " 中的 pv 数据是：" + count
                        );
                    }
                })
                .print();

        env.execute();
    }

    // Trigger接口第一个泛型是：流中元素的泛型
    // 第二个泛型是窗口的泛型
    public static class MyTrigger extends Trigger<Example2.Event, TimeWindow> {
        // 每来一条元素，触发执行一次
        // FIRE 触发的是ProcessWindowFunction中的process方法的执行
        // PURGE 会清空窗口中的元素
        // FIRE_PURGE 触发process执行并清空窗口中的元素
        @Override
        public TriggerResult onElement(Example2.Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 针对第一条数据，注册一个第一天哦啊数据的时间戳后面整数秒的定时器

            // 标志位状态变量，用来记录是否是第一个数据到达
            // 状态变量的作用域是当前窗口
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("is-first-event", Types.BOOLEAN)
            );

            if (isFirstEvent.value() == null) {
                // 首先计算第一条数据后面的整数秒
                long ts = element.ts + 1000L - element.ts % 1000L;
                // 注意！！！！！！！！！这里注册的定时器是`onEventTime()`方法
                ctx.registerEventTimeTimer(ts);
                // 这样后面到达的数据就不会运行条件分支语句了
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        // 机器时间到达`time`时，触发执行
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        // 水位线到达`time`时，触发执行
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (time < window.getEnd()) {
                // 注册一个下个整数秒的定时器
                // 注意！！！！！！这里注册的定时器还是onEventTime
                ctx.registerEventTimeTimer(time + 1000L);
                // 触发窗口计算，也就是输出此刻窗口中的 pv 数据
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        // clear方法在窗口销毁时调用
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // 标志位状态变量，用来记录是否是第一个数据到达
            // 单例
            //
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("is-first-event", Types.BOOLEAN)
            );
            isFirstEvent.clear();
        }
    }
}
