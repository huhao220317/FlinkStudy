package com.atguigu.day04;

import com.atguigu.day02.Example2;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// 模拟5秒钟的滚动窗口
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .keyBy(r -> r.url)
                .process(new FakeWindow(5000L))
                .print();

        env.execute();
    }

    public static class FakeWindow extends KeyedProcessFunction<String, Example2.Event, String> {
        private Long windowSize;

        public FakeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        // key是窗口开始时间
        // value是累加器
        private MapState<Long, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("windowstarttime-accumulator", Types.LONG, Types.LONG)
            );
        }

        @Override
        public void processElement(Example2.Event value, Context ctx, Collector<String> out) throws Exception {
            // 计算元素所属于的窗口的开始时间
            long currTs = ctx.timerService().currentProcessingTime();
            long windowStart = currTs - currTs % windowSize;

            // 如果mapState中不包含windowStart这个key，说明value是这个窗口的第一个元素
            // 对于事件时间，以如下逻辑来进行判断
            // if (ctx.timerService().currentWatermark() < windowEnd && !mapState.contains(windowStart))
            if (!mapState.contains(windowStart)) {
                // 创建新窗口和新的累加器
                mapState.put(windowStart, 1L);
            } else {
                // 来一条数据，累加器加一
                mapState.put(windowStart, mapState.get(windowStart) + 1L);
            }

            // 注册位于(窗口结束时间-1毫秒)的定时器
            ctx.timerService().registerProcessingTimeTimer(windowStart + windowSize - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long windowEnd = timestamp + 1L;
            long windowStart = windowEnd - windowSize;
            String url = ctx.getCurrentKey();
            long count = mapState.get(windowStart);
            out.collect(url + "在窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "的pv次数是：" + count);
            // 删除窗口
            mapState.remove(windowStart);
        }
    }
}
