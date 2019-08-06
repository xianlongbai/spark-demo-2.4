package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by root on 2019/7/31.
  */
class DataSetDemo {

}

object DataSetDemo{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("derby.log")
    rdd.take(1).foreach(println)

    val df: Dataset[String] = spark.read.textFile("derby.log")
    df.printSchema()

    val df2 = spark.read.parquet("test")
    df.printSchema()
    df2.show(false)
  }

}
