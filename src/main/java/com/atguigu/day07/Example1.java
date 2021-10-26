package com.atguigu.day07;

import com.atguigu.day03.Example9;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// 开关流
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Example9.SensorReading> sensorStream = env.addSource(new Example9.SensorSource());

        // 开关
        // 打开sensor_1的流，放行sensor_1的数据10秒钟，然后关闭sensor_1
        // 第二个字段是打开开关的时间长度
        DataStreamSource<Tuple2<String, Long>> switchStream = env
                .fromElements(Tuple2.of("sensor_1", 10 * 1000L));

        sensorStream.keyBy(r -> r.sensorId)
                .connect(switchStream.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Example9.SensorReading, Tuple2<String, Long>, Example9.SensorReading>() {
                    // 开关状态变量
                    private ValueState<Boolean> switchForwarding;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        switchForwarding = getRuntimeContext().getState(
                                new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN)
                        );
                    }

                    @Override
                    public void processElement1(Example9.SensorReading value, Context ctx, Collector<Example9.SensorReading> out) throws Exception {
                        // 如果开关是打开状态，就向下游发送数据
                        if (switchForwarding.value() != null && switchForwarding.value()) out.collect(value);
                    }

                    @Override
                    public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<Example9.SensorReading> out) throws Exception {
                        // 打开开关
                        switchForwarding.update(true);
                        // 注册一个关闭开关的定时器
                        // value.f1是打开开关的时间长度
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Example9.SensorReading> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        switchForwarding.clear();
                    }
                })
                .print();

        env.execute();
    }
}
