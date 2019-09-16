package com.sparkstreaming.operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2019/9/15.
  */
object dstream_output {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_transform")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Durations.seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    //wordCounts.print()
    //wordCounts.saveAsTextFiles("bxl","lol")
    //有2个参数,参数1：存储路径,最小目录为文件前缀,参数2:文件后缀
    //wordCounts.saveAsTextFiles("D:\\tmp\\sparkstreaming\\output\\result-part")
    wordCounts.saveAsObjectFiles("D:\\tmp\\sparkstreaming\\output2\\result-part")
    ssc.start()
    ssc.awaitTermination()

  }

}
