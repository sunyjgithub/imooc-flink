/*******************************************************************************
 * @(#)PKKafkaProducer.java 2021/10/12
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.imooc.flink.source.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.collection.mutable.StringBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/10/12 9:05
 */
public class PKKafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers","10.10.30.122:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        String topic = "syj_topic";

        while (true){
            StringBuilder stringBuilder =new StringBuilder();
            stringBuilder.append("imooc").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");
            System.out.println(stringBuilder.toString());
            producer.send(new ProducerRecord<String, String>(topic,stringBuilder.toString()));
            Thread.sleep(5000);
        }
    }

    private static long getTraffic() {
        return new Random().nextInt(10000);
    }

    private static String getDomains() {
        String[] domains = new String[]{
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "v5.go2yd.com"
              };
        return domains[new Random().nextInt(domains.length)];
    }

    private static String getIps() {
        String[] ips = new String[]{
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"};
        return ips[new Random().nextInt(ips.length)];
    }

    public static String getLevels() {
        String[] levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
