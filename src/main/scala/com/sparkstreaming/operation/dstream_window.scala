package com.sparkstreaming.operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2019/9/12.
  */
class dstream_window {

}
object dstream_window {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_union")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    ssc.checkpoint("D:\\tmp\\sparkstreaming\\checkpoint-003")

    val odstream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
//window（返回一个dstream）
//    val windowDst: DStream[String] = odstream.window(Durations.seconds(10),Durations.seconds(5))
//    val relDst: DStream[(String, Int)] = windowDst.flatMap(_.split(" ")).map((_,1))

//countByWindow(返回元素个数)  需要指定checkpoint
//    val relDst: DStream[Long] = odstream.countByWindow(Durations.seconds(10),Durations.seconds(5))

//reduceByWindow (如果使用优化版,则需要指定一个invReduceFunc函数，并且需要开启checkpoint)
      //val relDst: DStream[String] =odstream.reduceByWindow(_+"@"+_,Durations.seconds(10),Durations.seconds(5))

    //countByValueAndWindow(在（K，V）对的DStream上调用时，返回（K，Long）对的新DStream，其中每个键的值是其在滑动窗口内的频率)
    val kvDstream = odstream.flatMap(_.split(" ")).map((_,1))
    val relDst = kvDstream.countByValueAndWindow(Durations.seconds(10),Durations.seconds(5))
    relDst.print()

    ssc.start()
    ssc.awaitTermination()


  }

}