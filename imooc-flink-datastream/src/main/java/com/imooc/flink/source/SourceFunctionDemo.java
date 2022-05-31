/*******************************************************************************
 * @(#)SourceFunctionDemo.java 2022/5/30
 *
 * Copyright 2022 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.imooc.flink.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2022/5/30 8:04
 */
public class SourceFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        source.setParallelism(2);
        source.print();
        env.execute("SourceFunctionDemo");
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {

        private String id;

        private Integer userId;

        private Integer money;

        private Long createTime;
    }


    public static class MyOrderSource extends RichParallelSourceFunction<Order> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                // this synchronized block ensures that state checkpointing,
                // internal state updates and emission of elements are an atomic operation
                synchronized (ctx.getCheckpointLock()) {
                    String oid= UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(101);
                    long createTime = System.currentTimeMillis();
                    ctx.collect(new Order(oid,userId,money,createTime));
                    Thread.sleep(1000);
                }
            }
        }

        @Override
        public void cancel() {
            System.err.println("cancel");
            isRunning = false;
        }
    }
}
