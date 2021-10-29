package com.atguigu.day09;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

// 超时订单的检测
// 要求订单在5秒钟之内支付
// 也就是说创建订单和支付事件的时间间隔不能超过5s中
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<OrderEvent> stream = env
                .addSource(new SourceFunction<OrderEvent>() {
                    @Override
                    public void run(SourceContext<OrderEvent> ctx) throws Exception {
                        OrderEvent e1 = new OrderEvent(
                                "order-1",
                                "create",
                                1000L
                        );
                        ctx.collectWithTimestamp(e1, e1.ts);
                        Thread.sleep(1000L);
                        OrderEvent e2 = new OrderEvent(
                                "order-2",
                                "create",
                                2000L
                        );
                        ctx.collectWithTimestamp(e2, e2.ts);
                        Thread.sleep(1000L);

                        ctx.emitWatermark(new Watermark(3000L));

                        Thread.sleep(1000L);

                        OrderEvent e3 = new OrderEvent(
                                "order-1",
                                "pay",
                                4000L
                        );
                        ctx.collectWithTimestamp(e3, e3.ts);
                        Thread.sleep(1000L);

                        ctx.emitWatermark(new Watermark(6000L));

                        Thread.sleep(1000L);

                        ctx.emitWatermark(new Watermark(7000L));

                        Thread.sleep(1000L);

                        OrderEvent e4 = new OrderEvent(
                                "order-2",
                                "pay",
                                9000L
                        );

                        ctx.collectWithTimestamp(e4, e4.ts);

                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        stream
                .keyBy(r -> r.orderId)
                .process(new OrderTimeoutDetect())
                .print();

        env.execute();
    }

    public static class OrderTimeoutDetect extends KeyedProcessFunction<String, OrderEvent, String> {
        private ValueState<OrderEvent> valueState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("order-event", Types.POJO(OrderEvent.class))
            );
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if (value.type.equals("pay")) {
                out.collect("订单" + value.orderId + "支付成功");
                valueState.update(value); // 将pay事件保存下来
            } else if (value.type.equals("create")) {
                // 如果pay事件还没有到
                if (valueState.value() == null) {
                    valueState.update(value);
                    // 要求5秒钟之内支付
                    ctx.timerService().registerEventTimeTimer(value.ts + 5000L);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (valueState.value() != null && valueState.value().type.equals("create")) {
                out.collect("订单" + valueState.value().orderId + "没有在5秒钟之内支付");
            }
        }
    }

    public static class OrderEvent {
        public String orderId;
        public String type;
        public Long ts;

        public OrderEvent(String orderId, String type, Long ts) {
            this.orderId = orderId;
            this.type = type;
            this.ts = ts;
        }

        public OrderEvent() {
        }

        @Override
        public String toString() {
            return "OrderEvent{" +
                    "orderId='" + orderId + '\'' +
                    ", type='" + type + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }
}
