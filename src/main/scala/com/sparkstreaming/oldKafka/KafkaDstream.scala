package com.sparkstreaming.oldKafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2019/9/10.
  */
class KafkaDstream {

}
object KafkaDstream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("first")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true") //优雅的关闭程序
    val ssc = new StreamingContext(conf,Seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //第一个参数是StreamingContext
    //第二个参数是ZooKeeper集群信息（接受Kafka数据的时候会从Zookeeper中获得Offset等元数据信息）
    //第三个参数是Consumer Group
    //第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
    val topics = Map("test01" -> 2)
//    val receiveDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "192.168.249.102:2181,192.168.249.103:2181,192.168.249.104:2181", "spark_receiver", topics)
//    val orgDStream: DStream[(String, String)] = receiveDstream.cache()
//    val lines: DStream[String] = orgDStream.map(_._2)
//    val key: DStream[String] = orgDStream.map(_._1)
//    lines.flatMap(_.split(" ")).map((_,1)).print()
//    key.print()  //key是空的
//    ssc.start()
//    ssc.awaitTermination()


  }

}
