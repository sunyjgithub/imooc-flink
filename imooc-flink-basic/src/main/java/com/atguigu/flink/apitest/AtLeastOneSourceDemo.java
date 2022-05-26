/*******************************************************************************
 * @(#)AtLeastOneSourceDemo.java 2021/9/9
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/9 13:43
 */
public class AtLeastOneSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可以定期将状态保存到stateBackend 对状态做快照
        env.enableCheckpointing(30000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));



        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<String> errorData = lines1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    int i = 10 / 0;
                }
                return value;
            }
        });

        DataStreamSource<String> lines2 = env.addSource(new MyAtLeastOneSource("data2")).setParallelism(4);

        DataStream<String> union = errorData.union(lines2);

        union.print();
        env.execute();

    }
}
