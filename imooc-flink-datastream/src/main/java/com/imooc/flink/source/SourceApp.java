/*******************************************************************************
 * @(#)SourceApp.java 2021/8/21
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.imooc.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/8/21 9:31
 */
public class SourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment.createLocalEnvironment(3);
        // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);

        System.out.println("source:"+source.getParallelism());

        SingleOutputStreamOperator<Long> filterSource = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                System.out.println(Thread.currentThread().getName());
                return aLong > 5;
            }
        }).setParallelism(3);
        System.out.println("filterSource:"+ filterSource.getParallelism());

        filterSource.print();

        env.execute("nice");
    }
}
