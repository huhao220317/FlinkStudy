package com.atguigu.day02.ready;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Example02 {
    //
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5);
        ds
                .map(r -> Tuple2.of(r, r)).returns(Types.TUPLE(Types.INT, Types.INT))

                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, Tuple2<Integer, Integer>, String>() {
                    private ValueState<Tuple2<Integer, Integer>> statv;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        statv = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                                "max-min",
                                Types.TUPLE(Types.INT, Types.INT)
                        ));
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context ctx, Collector<String> out) throws Exception {

                        if (statv.value() == null) {
                            statv.update(Tuple2.of(
                                    value.f0,
                                    value.f1
                            ));
                        } else {
                            statv.update(
                                    Tuple2.of(
                                            value.f0 > statv.value().f0 ? value.f0 : statv.value().f0,
                                            value.f1 < statv.value().f1 ? value.f1 : statv.value().f1
                                    )
                            );
                        }
                        out.collect("最大值：" + statv.value().f0 + "， 最小值：" + statv.value().f1);
                    }
                }).print();

        env.execute();


    }
}
