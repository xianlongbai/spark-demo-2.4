package com.sparkstreaming.operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2019/9/10.
  */
class JoinDstream {}

object JoinDstream {

  def main(args: Array[String]): Unit = {
    //注意，这里有2个接口数据的线程，加上打印线程，需要启动三个线程
    val conf = new SparkConf().setMaster("local[3]").setAppName("dstream_foreach")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val receive1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",8888)
    val receive2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    val ss1: DStream[(String, Int)] = receive1.flatMap(_.split(" ")).map((_,1))
    val ss2: DStream[(String, Int)] = receive2.flatMap(_.split(" ")).map((_,1))

    val win1: DStream[(String, Int)] = ss1.window(Durations.seconds(20),Durations.seconds(5))
    val win2: DStream[(String, Int)] = ss2.window(Durations.seconds(20),Durations.seconds(5))

    val rel: DStream[(String, (Int, Int))] = win1.join(win2)

    rel.print()
    ssc.start()
    ssc.awaitTermination()

  }

}