/*******************************************************************************
 * @(#)BatchWordCount.java 2022/7/12
 *
 * Copyright 2022 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * wordcount 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2022/7/12 15:17
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = environment.readTextFile("data/wc.data");

        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);

        sum.print();
    }
}
