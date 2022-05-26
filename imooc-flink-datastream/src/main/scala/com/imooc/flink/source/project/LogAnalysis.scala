package com.imooc.flink.source.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * ${todo} 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/10/12 10:03
 */
object LogAnalysis {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val topic: String = "syj_topic";

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.10.30.122:9092")
    properties.setProperty("group.id", "sunyj-group")
    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)


    val dataSource = env.addSource(consumer)

    val logData: DataStream[(Long, String, Long)] = dataSource.map((x: String) => {
      val splits = x.split("\t")
      val level = splits(2)
      var time = 0L
      try {
        val dateFormata = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = dateFormata.parse(splits(3)).getTime
      } catch {
        case e: Exception => {
          println("出错了")
        }
      }
      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E").map(x => (x._2, x._3, x._4))

    val resultData: DataStream[(String, String, Long)] = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

      val maxOutOfOrderness = 10000L // 3.5 seconds

      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), recordTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1).window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(function = new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
          val domain = key.getField(0).toString
          var sum = 0L
          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._3
          }
          val start = window.getStart
          val end = window.getEnd
          val wStart = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(start)
          val wEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(end)
          out.collect((wStart + "-" + wEnd, domain, sum)
          )
        }
      })

    resultData.print()

    env.execute()


  }

}
