package com.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by root on 2019/7/21.
  *
  * rdd 转为 DataFrame
  */
class ChangeDataSet {

}

object ChangeDataSet{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val rdd: RDD[String] =  spark.sparkContext.textFile("people_df")
    import spark.implicits._

    val peopleDF: DataFrame = rdd.map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toLong)).toDF()
    peopleDF.createOrReplaceTempView("people")
    val teenagersDF: DataFrame = spark.sql("SELECT name, age FROM people")
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    teenagersDF.map(teenager => "Age: " + teenager.getAs[Int]("age")).show()

    //对于Dataset[Map[K,V]]没有预定义的编码器，需要显式定义,Any用来代替{String,Long}
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val res: Array[Map[String, Any]] = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    res.foreach(println)

  }

}
