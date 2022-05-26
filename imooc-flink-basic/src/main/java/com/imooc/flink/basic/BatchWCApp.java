/*******************************************************************************
 * @(#)BatchWCApp.java 2021/8/20
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.imooc.flink.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/8/20 23:10
 */
public class BatchWCApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.data");



        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] words = value.split(",");
                for(String word: words){
                    collector.collect(word.toLowerCase().trim());
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return StringUtils.isNotEmpty(s);
            }
        }).map(new RichMapFunction<String, Tuple2<String,Integer>>() {

            private transient ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println("初始化任务index=========="+indexOfThisSubtask);
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("count",Integer.class);
                valueState =getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                Integer historyCount = valueState.value();
                if (historyCount==null){
                    historyCount =0;
                }
                Integer result =  historyCount + 1;
                valueState.update(result);
                return Tuple2.of(value,result);
            }
        }).print();
    }

}
