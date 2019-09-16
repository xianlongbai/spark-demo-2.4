package com.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by root on 2019/9/2.
  */
class SaveDfTest {

}

object SaveDfTest{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val df = spark.read.json("people.txt")
    df.printSchema()

    //df.write.mode(SaveMode.Overwrite).option("sep","|").csv("D:\\tmp\\spark_sql\\csv")
    //df.rdd.map(row => row.getAs[String]("name")+"\\001"+row.getAs[Int]("age")).saveAsTextFile("D:\\tmp\\spark_sql\\csv")
  }

}
