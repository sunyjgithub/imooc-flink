/*******************************************************************************
 * @(#)ParallelismSourceApp.java 2023/1/3
 *
 * Copyright 2023 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.imooc.flink.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2023/1/3 9:28
 */
public class ParallelismSourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.fromParallelCollection(
                new NumberSequenceIterator(1, 10), Long.class
        );
        System.out.println("source Parallelism:" + source.getParallelism());

        SingleOutputStreamOperator<Long> filter = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 5;
            }
        }).setParallelism(6);

        System.out.println("filter Parallelism:" + filter.getParallelism());

        filter.print();

        env.execute("ParallelismSourceApp");

    }
}
