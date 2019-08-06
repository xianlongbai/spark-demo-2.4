package com.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

import scala.util.Try

/**
  * Created by root on 2019/7/19.
  */


case class SubRecord(x: Int)
case class ArrayElement(foo: String, bar: Int, vals: Array[Double])
case class TobRecord(
                   an_array: Array[Int],
                   a_map: Map[String, String],
                   a_struct: SubRecord,
                   an_array_of_structs: Array[ArrayElement]
                 )

//Scala的Seq将是Java的List，Scala的List将是Java的LinkedList。
object df_difficult {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val df: DataFrame = sc.parallelize(Seq(
      TobRecord(
        Array(1, 2, 3),
        Map("foo" -> "bar"),
        SubRecord(1),
        Array(ArrayElement("foo", 1, Array(1.0, 2.0)), ArrayElement("bar", 2, Array(3.0, 4.0)))
      ),
      TobRecord(
        Array(4, 5, 6),
        Map("foz" -> "baz"),
        SubRecord(2),
        Array(ArrayElement("foz", 3, Array(5.0, 6.0)), ArrayElement("baz", 4, Array(7.0, 8.0))))
    )).toDF

    df.createTempView("df_table")
    df.printSchema()
//    df.show(false)




  }







}
