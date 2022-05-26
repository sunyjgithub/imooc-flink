/*******************************************************************************
 * @(#)TumblingWindowAll.java 2021/9/11
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/11 9:46
 */
public class TumblingWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // 1
        // 2
        // 3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        AllWindowedStream<Integer, TimeWindow> window = nums.timeWindowAll(Time.seconds(5));

        SingleOutputStreamOperator<Integer> sum = window.sum(0);

        sum.print();

        env.execute();
    }
}
