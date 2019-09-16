package com.sparkstreaming.operation

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by root on 2019/9/15.
  */

case class Record(word: String)
/** Lazily instantiated singleton instance of SparkSession */

object SparkSessionSingleton {
  //不需要序列化
  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}



object dstream_dataframe {

  def main(args: Array[String]): Unit = {
      // Create the context with a 2 second batch size
      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SqlNetworkWordCount")
      val ssc = new StreamingContext(sparkConf, Seconds(10))

      val lines = ssc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.flatMap(_.split(" "))

      // Convert RDDs of the words DStream to DataFrame and run SQL query
      words.foreachRDD { (rdd: RDD[String], time: Time) =>
        // Get the singleton instance of SparkSession
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._

        // Convert RDD[String] to RDD[case class] to DataFrame
        val wordsDataFrame = rdd.map(w => Record(w)).toDF()

        // Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        // Do word count on table using SQL and print it
        val wordCountsDataFrame =
          spark.sql("select word, count(*) as total from words group by word")
        println(s"========= $time =========")
        wordCountsDataFrame.show()
      }

      ssc.start()
      ssc.awaitTermination()
    }

}
