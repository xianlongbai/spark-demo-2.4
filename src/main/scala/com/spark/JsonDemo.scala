package com.spark

import com.fasterxml.jackson.annotation.JsonFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2019/9/12.
  */
class JsonDemo {

}
object JsonDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    val jsonRdd: RDD[String] = sc.textFile("people.txt")



  }
}