package com.atguigu.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// ValueState的使用
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, Integer, String>() {
                    // 声明了一个状态变量
                    // 元组第一个元素是最小值，第二个元素是最大值
                    private ValueState<Tuple2<Integer, Integer>> minMaxTuple;
                    private ValueState<Tuple2<Integer, Integer>> sumCountTuple;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态变量
                        minMaxTuple = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                                        "min-max-tuple",
                                        Types.TUPLE(Types.INT, Types.INT)
                                )
                        );
                        sumCountTuple = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                                        "sum-count-tuple",
                                        Types.TUPLE(Types.INT, Types.INT)
                                )
                        );
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        if (minMaxTuple.value() == null) {
                            // 状态变量为空，则将第一条数据保存到状态变量中
                            minMaxTuple.update(Tuple2.of(value, value));
                        } else {
                            Tuple2<Integer, Integer> temp = minMaxTuple.value();
                            minMaxTuple.update(Tuple2.of(
                                    temp.f0 > value ? value : temp.f0,
                                    temp.f1 > value ? temp.f1 : value
                            ));
                        }

                        out.collect("最大值是：" + minMaxTuple.value().f1 + "; 最小值是：" + minMaxTuple.value().f0);

                        // ======================
                        if (sumCountTuple.value() == null) {
                            sumCountTuple.update(Tuple2.of(value, 1));
                        } else {
                            Tuple2<Integer, Integer> temp = sumCountTuple.value();
                            sumCountTuple.update(Tuple2.of(
                                    temp.f0 + value,
                                    temp.f1 + 1
                            ));
                        }

                        out.collect("平均值是：" + sumCountTuple.value().f0 / sumCountTuple.value().f1);
                    }
                })
                .print();

        env.execute();
    }
}
