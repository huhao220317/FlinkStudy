package com.atguigu.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Random;

public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TempIncreaseAlert())
                .print();

        env.execute();
    }

    public static class TempIncreaseAlert extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Double> prevTemp;
        private ValueState<Long> timerTs;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            prevTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("prev-temp", Types.DOUBLE));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Types.LONG));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = null;
            if (prevTemp.value() != null) lastTemp = prevTemp.value();
            prevTemp.update(value.temperature);

            Long ts = null;
            if (timerTs.value() != null) ts = timerTs.value();

            // 1 2 3 4 5 6 1
            if (lastTemp != null && value.temperature < lastTemp) {
                if (ts != null) {
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    timerTs.clear();
                }
            } else if (lastTemp != null && value.temperature > lastTemp && ts == null) {
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + 1000L
                );
                timerTs.update(ctx.timerService().currentProcessingTime() + 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器" + ctx.getCurrentKey() + "连续一秒钟温度上升了！");
            timerTs.clear();
        }
    }

    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        private String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                long currTime = Calendar.getInstance().getTimeInMillis();
                for (int i = 0; i < 3; i++) {
                    ctx.collect(new SensorReading(
                            "sensor_" + (i + 1),
                            (double)random.nextInt(1000),
                            currTime
                    ));
                }
                Thread.sleep(400L);
            }
//            ctx.collect(new SensorReading("sensor_1", 1.0, 1000L));
//            Thread.sleep(300L);
//            ctx.collect(new SensorReading("sensor_1", 2.0, 1000L));
//            Thread.sleep(300L);
//            ctx.collect(new SensorReading("sensor_1", 3.0, 1000L));
//            Thread.sleep(300L);
//            ctx.collect(new SensorReading("sensor_1", 4.0, 1000L));
//            Thread.sleep(3000L);
//            ctx.collect(new SensorReading("sensor_1", 5.0, 1000L));
//            Thread.sleep(300L);
//            ctx.collect(new SensorReading("sensor_1", 6.0, 1000L));
//            Thread.sleep(300L);
//            ctx.collect(new SensorReading("sensor_1", 7.0, 1000L));
//            Thread.sleep(300L);
//            ctx.collect(new SensorReading("sensor_1", 8.0, 1000L));
//            Thread.sleep(3000L);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class SensorReading {
        public String sensorId;
        public Double temperature;
        public Long ts;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature, Long ts) {
            this.sensorId = sensorId;
            this.temperature = temperature;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", temperature=" + temperature +
                    ", ts=" + ts +
                    '}';
        }
    }
}
