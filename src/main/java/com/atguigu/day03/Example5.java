package com.atguigu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("并行任务ID为：" + getRuntimeContext().getIndexOfThisSubtask() + " 的生命周期开始了");
                    }

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i < 9; i++) {
                            int subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
                            if (i % 2 == subTaskIdx) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("并行任务ID为：" + getRuntimeContext().getIndexOfThisSubtask() + " 的生命周期结束了");
                    }
                })
                .setParallelism(2)
                // .rebalance
                .rescale()
                .print()
                .setParallelism(4);

        env.execute();
    }
}
