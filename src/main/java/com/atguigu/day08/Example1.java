package com.atguigu.day08;

import com.atguigu.day02.Example2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 每隔10秒钟保存一次检查点，或者说保存一次状态
        env.enableCheckpointing(10 * 1000L);
        // 设置检查点的文件夹的路径
        // file:// + 绝对路径
        // flink 默认只保存最近一次的检查点
        env.setStateBackend(new FsStateBackend("file:///home/zuoyuan/flink0519tutorial/src/main/resources/ckpts"));

        env
                .addSource(new Example2.ClickSource())
                .print();

        env.execute();
    }
}
