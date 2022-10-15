/*******************************************************************************
 * @(#)BatchWordCount.java 2022/7/12
 *
 * Copyright 2022 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * wordcount 批流一体处理方式
 * environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
 *
 * 流处理结果
 * 7> (flink,1)
 * 7> (flink,2)
 * 3> (pk,1)
 * 3> (pk,2)
 * 3> (pk,3)
 * 3> (pk,4)
 * 3> (pk,5)
 * 3> (pk,6)
 * 批处理结果
 * 7> (flink,2)
 * 3> (pk,6)
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2022/7/12 15:17
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //切换成批处理模式
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> dataSource = environment.readTextFile("data/wc.data");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        environment.execute();

    }

}
