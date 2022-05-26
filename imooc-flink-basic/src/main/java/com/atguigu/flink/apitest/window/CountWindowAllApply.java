/*******************************************************************************
 * @(#)CountWindowAll.java 2021/9/9
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/9 17:12
 */
public class CountWindowAllApply {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1
        // 2
        // 3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        AllWindowedStream<Integer, GlobalWindow> allWindowedStream = nums.countWindowAll(5);

        allWindowedStream.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                        int sum  =  0;
                        for (Integer value: values){
                            sum =  sum + value;
                        }
                        out.collect(sum);
            }
        }).print();

        env.execute();

    }
}
