/*******************************************************************************
 * @(#)TopNTest.java 2021/9/7
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/7 20:09
 */
public class TopNTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<MyBehavior> process = lines.process(new ProcessFunction<String, MyBehavior>() {
            @Override
            public void processElement(String input, Context ctx, Collector<MyBehavior> out) throws Exception {
                try {
                    // FastJson 会自动把时间解析成long类型的TimeStamp
                    MyBehavior behavior = JSON.parseObject(input, MyBehavior.class);
                    out.collect(behavior);
                } catch (Exception e) {
                    e.printStackTrace();
                    //TODO 记录出现异常的数据
                }
            }
        });

        //      设定延迟时间
        SingleOutputStreamOperator<MyBehavior> behaviorDSWithWaterMark =
                process.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyBehavior>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(MyBehavior element) {
                        return element.timestamp;
                    }
                });

        //  某个商品，在窗口时间内，被（点击、购买、添加购物车、收藏）了多少次
        KeyedStream<MyBehavior, Tuple> keyed = behaviorDSWithWaterMark.keyBy("itemId", "type");

        WindowedStream<MyBehavior, Tuple, TimeWindow> window =
                keyed.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));

        SingleOutputStreamOperator<ItemViewCount> result = window.aggregate(new MyWindowAggFunction(),
                new MyWindowFunction());

        result.print();
        env.execute("HotGoodsTopN");
    }

    public static class MyWindowAggFunction implements AggregateFunction<MyBehavior, Long, Long> {

        //初始化一个计数器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        //每输入一条数据就调用一次add方法
        @Override
        public Long add(MyBehavior input, Long accumulator) {
            return accumulator + input.counts;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        //只针对SessionWindow有效，对应滚动窗口、滑动窗口不会调用此方法
        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }


    public static class MyWindowFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            String itemId = tuple.getField(0);
            String type = tuple.getField(1);

            long windowStart = window.getStart();
            long windowEnd = window.getEnd();

            //窗口集合的结果
            Long aLong = input.iterator().next();

            //输出数据
            out.collect(ItemViewCount.of(itemId, type, windowStart, windowEnd, aLong));
        }
    }
}
