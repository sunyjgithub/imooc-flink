/*******************************************************************************
 * @(#)MapStateDemo.java 2021/9/9
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/9 8:52
 */
public class ListStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<Tuple2<String, String>> tpDataStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        KeyedStream<Tuple2<String, String>, String> keyedStream = tpDataStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, List<String>>> result = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>>() {

            private transient ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("list-state", String.class);
                listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
                String action = value.f1;
                listState.add(action);
                Iterable<String> iterable = listState.get();
                List<String> events = new ArrayList<>();
                for (String name : iterable) {
                    events.add(name);
                }
                out.collect(Tuple2.of(value.f0, events));
            }
        });


        result.print();

        env.execute("liststatetest");
    }
}
