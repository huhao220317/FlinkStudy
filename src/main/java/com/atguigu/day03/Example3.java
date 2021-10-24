package com.atguigu.day03;

import com.atguigu.day02.Example2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// sink to mysql
// create database userbehavior;
// create table clicks (username: varchar(100), url: varchar(100));
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.ClickSource())
                .addSink(new RichSinkFunction<Example2.Event>() {
                    private Connection conn;
                    private PreparedStatement insertStmt;
                    private PreparedStatement updateStmt;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        conn = DriverManager.getConnection(
                                "jdbc:mysql://localhost:3306/userbehavior?useSSL=false",
                                "root",
                                "root"
                        );
                        insertStmt = conn.prepareStatement("INSERT INTO clicks (username, url) VALUES (?, ?)");
                        updateStmt = conn.prepareStatement("UPDATE clicks SET url = ? WHERE username = ?");
                    }

                    // 来一条数据，触发一次调用
                    @Override
                    public void invoke(Example2.Event value, Context context) throws Exception {
                        super.invoke(value, context);
                        updateStmt.setString(1, value.url);
                        updateStmt.setString(2, value.user);
                        updateStmt.execute();

                        if (updateStmt.getUpdateCount() == 0) {
                            insertStmt.setString(1, value.user);
                            insertStmt.setString(2, value.url);
                            insertStmt.execute();
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        insertStmt.close();
                        updateStmt.close();
                        conn.close();
                    }
                });

        env.execute();
    }
}
