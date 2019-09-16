package com.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by root on 2019/9/10.
  */
class QueueDstream {

}

object QueueDstream {

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("txt")
//    val ssc = new StreamingContext(conf,Seconds(10))
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    val  sc = ssc.sparkContext
//    sc.setLogLevel("error")
//
//  }
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("RDD队列流").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(3))   //每一秒监听一次
    val RDDQueue=new mutable.Queue[RDD[Int]]
    val queueStream=ssc.queueStream(RDDQueue)
    val result: DStream[(Int, Int)] =queueStream.map(x=>(x%5,1)).reduceByKey(_+_)
    result.print(1000)  //take(1000)
    ssc.start()

    while(true){
      RDDQueue +=ssc.sparkContext.makeRDD(1 to 100,2)
      Thread.sleep(2000)    //每2秒发一次数据
    }
    ssc.stop()
  }


}
