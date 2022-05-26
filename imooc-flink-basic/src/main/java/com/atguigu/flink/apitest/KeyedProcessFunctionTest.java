/*******************************************************************************
 * @(#)KeyedProcessFunctionTest.java 2021/9/7
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/7 15:30
 */
public class KeyedProcessFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.keyBy("sensorId")
                  .process(new KeyedProcessFunction<Tuple, SensorReading, Integer>() {
                      @Override
                      public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
//                          System.out.println("current-key:"+ ctx.getCurrentKey().getField(0));
                          System.out.println(Thread.currentThread().getId());
                          out.collect(Integer.valueOf(value.getSensorId()));
                      }
                  }).print().setParallelism(1);

        env.execute();
    }

    public static class SensorReading{

        // 传感器 id
        public String sensorId;
        // 时间戳
        public Long timeStamp;
        // 温度
        public Double temperature;

        /**
         * 获取sensorId字段数据
         *
         * @return Returns the sensorId.
         */
        public String getSensorId() {
            return sensorId;
        }

        /**
         * 设置sensorId字段数据
         *
         * @param sensorId The sensorId to set.
         */
        public void setSensorId(String sensorId) {
            this.sensorId = sensorId;
        }

        /**
         * 获取timeStamp字段数据
         *
         * @return Returns the timeStamp.
         */
        public Long getTimeStamp() {
            return timeStamp;
        }

        /**
         * 设置timeStamp字段数据
         *
         * @param timeStamp The timeStamp to set.
         */
        public void setTimeStamp(Long timeStamp) {
            this.timeStamp = timeStamp;
        }

        /**
         * 获取temperature字段数据
         *
         * @return Returns the temperature.
         */
        public Double getTemperature() {
            return temperature;
        }

        /**
         * 设置temperature字段数据
         *
         * @param temperature The temperature to set.
         */
        public void setTemperature(Double temperature) {
            this.temperature = temperature;
        }

        public SensorReading(String sensorId, Long timeStamp, Double temperature){
            this.sensorId = sensorId;
            this.timeStamp = timeStamp;
            this.temperature = temperature;
        }

        public SensorReading() {

        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", timeStamp=" + timeStamp +
                    ", temperature=" + temperature +
                    '}';
        }
    }
}

