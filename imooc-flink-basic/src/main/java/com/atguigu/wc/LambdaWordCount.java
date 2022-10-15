/*******************************************************************************
 * @(#)BatchWordCount.java 2022/7/12
 *
 * Copyright 2022 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * wordcount 使用lambda表达式进行算子的编写，代替匿名内部类
 * 这里面涉及到数据类型的处理flink
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2022/7/12 15:17
 */
public class LambdaWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamContextEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataSource = environment.readTextFile("data/wc.data");

        // 先变大写
        SingleOutputStreamOperator<String> map = dataSource.map(String::toUpperCase);


        map.flatMap((value,out)->{
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT));




        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);

        sum.print();
    }

}
