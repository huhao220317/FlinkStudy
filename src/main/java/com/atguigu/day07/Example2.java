package com.atguigu.day07;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

// 实时对账
// 第一条流：app端支付流
// 第二条流：第三方支付流
// 只有当app服务端接受到来自第三方支付的回调确认信息时，才能确认订单已支付
// 微信支付和银行卡支付的实时对账
// 订单表和订单详情表的实时对账
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // app支付流
        DataStreamSource<Event> appStream = env
                // 可以在自定义数据源中指定事件的时间戳，以及发送水位线
                // 在自定义数据源中如果指定了时间戳和发送水位线之后，就不能使用
                // assignTimestampAndWatermarks方法了
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        Event e1 = new Event("order-1", "app-zhifu", 1000L);
                        // e1.ts指定了e1的时间戳是e1.ts
                        // 第一个参数是要发送的数据
                        // 第二个参数是要发送数据的时间戳
                        ctx.collectWithTimestamp(e1, e1.ts);
                        // 发送水位线
                        ctx.emitWatermark(new Watermark(e1.ts - 1L));
                        Event e2 = new Event("order-2", "app-zhifu", 2000L);
                        ctx.collectWithTimestamp(e2, e2.ts);
                        ctx.emitWatermark(new Watermark(e2.ts - 1L));
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        // weixin支付流
        DataStreamSource<Event> weixinStream = env
                // 可以在自定义数据源中指定事件的时间戳，以及发送水位线
                // 在自定义数据源中如果指定了时间戳和发送水位线之后，就不能使用
                // assignTimestampAndWatermarks方法了
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        Event e1 = new Event("order-1", "weixin-zhifu", 4000L);
                        // e1.ts指定了e1的时间戳是e1.ts
                        // 第一个参数是要发送的数据
                        // 第二个参数是要发送数据的时间戳
                        ctx.collectWithTimestamp(e1, e1.ts);
                        // 发送水位线
                        ctx.emitWatermark(new Watermark(e1.ts - 1L));
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                        Event e2 = new Event("order-2", "weixin-zhifu", 9000L);
                        ctx.collectWithTimestamp(e2, e2.ts);
                        ctx.emitWatermark(new Watermark(e2.ts - 1L));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        appStream.keyBy(r -> r.orderId)
                .connect(weixinStream.keyBy(r -> r.orderId))
                .process(new MatchProcess())
                .print();

        env.execute();
    }

    public static class MatchProcess extends CoProcessFunction<Event, Event, String> {
        // 用来保存app的支付事件，如果app的支付事件率先到达，就保存下来
        private ValueState<Event> appState;
        // 用来保存weixin的支付事件，如果weixin的支付事件率先到达，就保存下来
        private ValueState<Event> weixinState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            appState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("app-zhifu", Types.POJO(Event.class))
            );
            weixinState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("weixin-state", Types.POJO(Event.class))
            );
        }

        @Override
        public void processElement1(Event value, Context ctx, Collector<String> out) throws Exception {
            // 当app-zhifu到达时
            // 首先检查weixin-state有没有数据，如果有数据，说明weixin支付事件先到达
            if (weixinState.value() != null) {
                // 如果weixinState中有数据，说明对账成功
                out.collect("订单号：" + value.orderId + "对账成功，微信支付先到达的，app支付后到达的。");
                // 清空weixin-state
                weixinState.clear();
            } else {
                // app支付先到达了,将app支付保存下来
                appState.update(value);
                // 注册一个5秒钟之后的定时器
                ctx.timerService().registerEventTimeTimer(value.ts + 5000L);
            }
        }

        @Override
        public void processElement2(Event value, Context ctx, Collector<String> out) throws Exception {
            // 当weixin-zhifu到达时
            // 首先检查app-state有没有数据，如果有数据，说明app支付事件先到达
            if (appState.value() != null) {
                // 如果appState中有数据，说明对账成功
                out.collect("订单号：" + value.orderId + "对账成功，app支付先到达的，微信支付后到达的。");
                // 清空app-state
                appState.clear();
            } else {
                // weixin支付先到达了,将weixin支付保存下来
                weixinState.update(value);
                // 注册一个5秒钟之后的定时器
                ctx.timerService().registerEventTimeTimer(value.ts + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 如果定时器触发时，appState不为空，说明对应的weixin支付没有来
            if (appState.value() != null) {
                out.collect("订单号：" + appState.value().orderId + " 对账失败，app支付到达了，weixin支付未到达");
                appState.clear();
            }
            if (weixinState.value() != null) {
                out.collect("订单号：" + weixinState.value().orderId + " 对账失败，weixin支付到达了，app支付未到达");
                weixinState.clear();
            }
        }
    }

    public static class Event {
        public String orderId;
        public String type;
        public Long ts;

        public Event() {
        }

        public Event(String orderId, String type, Long ts) {
            this.orderId = orderId;
            this.type = type;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "orderId='" + orderId + '\'' +
                    ", type='" + type + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }
}
