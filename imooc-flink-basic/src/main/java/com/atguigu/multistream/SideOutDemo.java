/*******************************************************************************
 * @(#)BatchWordCount.java 2022/7/12
 *
 * Copyright 2022 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.multistream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 测流输出 分流操作  用到processFunction
 * 这里面涉及到数据类型的处理flink
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2022/7/12 15:17
 */
public class SideOutDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataSource = env.readTextFile("data/wc.data");

        // 先变大写
        SingleOutputStreamOperator<String> map = dataSource.map(String::toUpperCase);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = map.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        OutputTag outputTag1 = new OutputTag<Tuple2<String,Integer>>("pk"){};
        OutputTag outputTag2 = new OutputTag<Tuple2<String,Integer>>("flink"){};

        // For every element in the input stream {@link #processElement(Object, Context, Collector)} is  invoked.
        // This can produce zero or more elements as output
        SingleOutputStreamOperator<Tuple2<String, Integer>> main = wordAndOne.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if ("pk".equalsIgnoreCase(value.f0)) {
                    ctx.output(outputTag1, value);
                }
                if ("flink".equalsIgnoreCase(value.f0)) {
                    ctx.output(outputTag2, value);
                }
            }
        });


        DataStream sideOutputStream = main.getSideOutput(outputTag1);

        sideOutputStream.print();

        env.execute("sideout");

    }

}
