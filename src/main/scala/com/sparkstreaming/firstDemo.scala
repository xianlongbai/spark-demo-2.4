package com.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//请注意是apache.log4j不是org.slf4j
import org.apache.log4j.{Level, Logger}

/**
  * Created by root on 2019/9/6.
  *
  * linux : nc -lk 9999
  * windows : nc -l -p 9999
  */
class firstDstream{

}

object firstDstream {

  def main(args: Array[String]): Unit = {
    //这里至少得配置两个线程才能打印出相关数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("first")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true") //优雅的关闭程序
    val ssc = new StreamingContext(conf,Seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

    //置为false,应用程序只停止StreamingContext
    ssc.stop(false)

  }

}
