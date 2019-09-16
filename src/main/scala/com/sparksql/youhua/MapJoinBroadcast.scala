package com.sparksql.youhua

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.broadcast

/**
  * Created by root on 2019/8/20.
  */
class MapJoinBroadcast {

}

object MapJoinBroadcast{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val devId = spark.read.textFile("tdid.txt").toDF("tdid")
    val tdid = broadcast(devId)
    val df01 = spark.read.parquet("test").toDF("tdid", "applist")
    val df02: DataFrame = spark.read.parquet("test2").toDF("tdid", "applist")





  }

}
