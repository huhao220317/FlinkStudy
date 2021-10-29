package com.atguigu.day09;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

// 超时订单的检测
// 要求订单在5秒钟之内支付
// 也就是说创建订单和支付事件的时间间隔不能超过5s中
public class Example3 {
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

                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.type.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.type.equals("pay");
                    }
                })
                // 要求两个事件在5秒钟之内匹配
                .within(Time.seconds(5));

        PatternStream<OrderEvent> patternStream = CEP.pattern(
                stream.keyBy(r -> r.orderId),
                pattern
        );

        SingleOutputStreamOperator<String> result = patternStream
                // flatSelect可以向下游发送多条数据
                .flatSelect(
                        // 侧输出流用来接收超时未支付的订单信息
                        new OutputTag<String>("unpayed-order") {
                        },
                        // 用来将超时未支付的订单信息发送到侧输出流
                        new PatternFlatTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public void timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                                // 匹配到的订单事件只有create事件，没有pay事件
                                OrderEvent create = pattern.get("create").get(0);
                                // 注意collect方法将信息发送到了上面定义的侧输出流中
                                out.collect("订单" + create.orderId + "超时未支付");
                            }
                        },
                        // 用来发送正常支付的订单信息
                        new PatternFlatSelectFunction<OrderEvent, String>() {
                            @Override
                            public void flatSelect(Map<String, List<OrderEvent>> pattern, Collector<String> out) throws Exception {
                                OrderEvent create = pattern.get("create").get(0);
                                OrderEvent pay = pattern.get("pay").get(0);
                                // 正常支付的订单，既有创建订单的事件，也有支付事件
                                out.collect("订单" + create.orderId + "在" +
                                        "" + create.ts + " 创建订单，在" +
                                        "" + pay.ts + " 支付订单");
                            }
                        }
                );

        result.print();

        result.getSideOutput(new OutputTag<String>("unpayed-order"){}).print();

        env.execute();
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
