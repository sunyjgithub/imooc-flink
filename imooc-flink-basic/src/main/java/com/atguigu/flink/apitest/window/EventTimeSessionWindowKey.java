/*******************************************************************************
 * @(#)CountWindowAll.java 2021/9/9
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 这里请补充该类型的简述说明
 * 同一个组的到达指定的条数才会触发
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/9 17:12
 */
public class EventTimeSessionWindowKey {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1000,spark,3    1970-01-01 08:00:01
        // 1000,spark,5
        // 1100,hadoop,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<String> linesWithTimeStamp = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple3<Long,String, Integer>> wordAndCount = linesWithTimeStamp.map(new MapFunction<String, Tuple3<Long,String, Integer>>() {
            @Override
            public Tuple3<Long,String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Long.parseLong(fields[0]),fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple3<Long,String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f1);
        WindowedStream<Tuple3<Long,String, Integer>, String, TimeWindow> windowed =keyed.window(EventTimeSessionWindows.withGap(Time.seconds(10)));

        windowed.sum(2).print();


        env.execute();

    }
}
