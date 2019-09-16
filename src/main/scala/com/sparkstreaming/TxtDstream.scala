package com.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2019/9/9.
  *
  * 监控 本地/hdfs 的数据目录，不支持监控文件的追加
  *
  * SparkStream 监控文件目录时，只能监控文件内是否添加新的文件，
  * 如果文件名没有改变只是文件内容改变，那么不会检测出有文件进行了添加。
  *
  * 注意：
  *   这路径如果hdfs的路径 你直接hadoop fs  -put 到你的监测路径就可以，
  *   如果是本地目录用file:///home/data 你不能移动文件到这个目录，必须用流的形式写入到这个目录形成文件才能被监测到。
  */
class TxtDstream {}

object TxtDstream {

  def main(args: Array[String]): Unit = {
    //这里至少得配置两个线程才能打印出相关数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("txt")
    val ssc = new StreamingContext(conf,Seconds(10))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val txtDs: DStream[String] = ssc.textFileStream("D:\\tmp\\sparkstreaming\\jiankong")
    txtDs.print()
    //    val words = txtDs.flatMap(_.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
//    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
