/*******************************************************************************
 * @(#)GroupedProcessingTimeWindowExample.java 2022/5/30
 *
 * Copyright 2022 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.imooc.flink.source;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2022/5/30 18:20
 */
public class GroupedProcessingTimeWindowExample {

    public static class DataSource extends RichParallelSourceFunction<Tuple2<String,Integer>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String,Integer>> ctx) throws Exception {
            Random random = new Random(System.currentTimeMillis());
            while (running){
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 +500);
                String key =  "类别" + (char)('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;
                System.out.println(String.format("emit: \t(%s,%d)",key,value));
                ctx.collect(new Tuple2<>(key,value));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new DataSource());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = source.keyBy(0);

        keyedStream.sum(1).print();
        source.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.println(String.format("get:\t(%s,%d)",value.f0,value.f1));
            }
        });
        env.execute();
    }
}
