package com.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by root on 2019/7/21.
  *
  * 通过RDD[Row]+Schema 构建 DataFrame
  */
class CreateDfBySchema {

}

object CreateDfBySchema{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("people_df")

    val schemaString = "name age"
    val fields: Array[StructField] = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD: RDD[Row] = peopleRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDf: DataFrame = spark.createDataFrame(rowRDD,schema)

    peopleDf.createTempView("people")

    spark.sql("select * from people").show(false)

  }

}
