package com.atguigu.day04;

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

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    private ValueState<Double> lastTemp;
                    private ValueState<Long> timerTs;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                        Double prevTemp = null;
                        if (lastTemp.value() != null) prevTemp = lastTemp.value();
                        lastTemp.update(value.temperature);

                        Long ts = null;
                        if (timerTs.value() != null) ts = timerTs.value();

                        if (prevTemp != null && value.temperature < prevTemp && ts != null) {
                            ctx.timerService().deleteProcessingTimeTimer(ts);
                            timerTs.clear();
                        } else if (prevTemp != null && value.temperature > prevTemp && ts == null) {
                            long oneSecLater = ctx.timerService().currentProcessingTime() + 1000L;
                            ctx.timerService().registerProcessingTimeTimer(oneSecLater);
                            timerTs.update(oneSecLater);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("传感器" + ctx.getCurrentKey() + "温度连续1秒钟上升");
                        timerTs.clear();
                    }
                })
                .print();
        
        env.execute();
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
                Thread.sleep(300L);
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
