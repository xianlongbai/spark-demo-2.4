package com.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2019/7/20.
  */
object SimpleApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Simple Application").master("local[2]").getOrCreate()

    val logData = spark.read.textFile("simple.txt").cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    //while (true){}
    spark.stop()
  }


}
