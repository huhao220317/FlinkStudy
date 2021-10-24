package com.atguigu.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(
                        Tuple2.of("user-1", "fail"),
                        Tuple2.of("user-2", "success"),
                        Tuple2.of("user-1", "fail"),
                        Tuple2.of("user-1", "fail"),
                        Tuple2.of("user-1", "fail")
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, String>() {
                    private HashMap<Tuple2<String, String>, String> stateMachine = new HashMap<>();

                    private ValueState<String> currentState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stateMachine.put(Tuple2.of("INITIAL", "fail"), "S1");
                        stateMachine.put(Tuple2.of("INITIAL", "success"), "SUCCESS");
                        stateMachine.put(Tuple2.of("S1", "fail"), "S2");
                        stateMachine.put(Tuple2.of("S2", "fail"), "FAIL");
                        stateMachine.put(Tuple2.of("S1", "success"), "SUCCESS");
                        stateMachine.put(Tuple2.of("S2", "success"), "SUCCESS");

                        currentState = getRuntimeContext().getState(new ValueStateDescriptor<String>("current-state", Types.STRING));
                    }

                    @Override
                    public void processElement(Tuple2<String, String> in, Context ctx, Collector<String> out) throws Exception {
                        if (currentState.value() == null) currentState.update("INITIAL");

                        String nextState = stateMachine.get(Tuple2.of(currentState.value(), in.f1));
                        if (nextState.equals("FAIL")) {
                            out.collect("用户" + in.f0 + "连续三次登录失败");
                            currentState.update("S2");
                        } else if (nextState.equals("SUCCESS")) {
                            currentState.update("INITIAL");
                        } else {
                            currentState.update(nextState);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
