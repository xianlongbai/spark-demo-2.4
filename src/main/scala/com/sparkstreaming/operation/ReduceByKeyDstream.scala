package com.sparkstreaming.operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * Created by root on 2019/9/12.
  *
  */
object ReduceByKeyDstream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_foreach")
    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    conf.set("spark.streaming.backpressure.enabled","true")
    conf.set("spark.streaming.blockInterval","100") //100ms

    val ssc = new StreamingContext(conf,Durations.seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //每一个batch 都会记录
    ssc.checkpoint("D:\\tmp\\sparkstreaming\\checkpoint-001")
    //开启日志预写的情况下,这里数据接收的持久化级别就不需要,StorageLevel.MEMORY_ONLY_SER_2了
    val receive: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY_SER)
    val preRdd: DStream[(String, Int)] = receive.flatMap(_.split(" ")).map((_,1))

    //每隔10秒计算一次最近20秒的数据
    //val res1: DStream[(String, Int)] = preRdd.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(5))

    //使用优化版的reduceByKeyAndWindow，必须设置检查点，负责程序启动报错
    val res2: DStream[(String, Int)] = preRdd.reduceByKeyAndWindow((a: Int, b: Int) =>a+b, (c:Int, d:Int) => c-d, Seconds(20), Seconds(5))

    res2.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
