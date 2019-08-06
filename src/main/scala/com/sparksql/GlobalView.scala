package com.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by root on 2019/7/21.
  */
class GlobalView {

}

object GlobalView{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val df: DataFrame = spark.read.json("udaf.txt")
    df.printSchema()
    //Spark SQL中的临时视图是会话范围的,如果创建它的会话终止,它将消失
    df.createTempView("person")
    //如果您希望拥有一个在所有会话之间共享的临时视图并保持活动状态，直到Spark应用程序终止，您可以创建一个全局临时视图
    df.createGlobalTempView("person")

    //删除视图
//    spark.catalog.dropTempView("person")
//    spark.catalog.dropGlobalTempView("person")

    val res0: DataFrame = spark.sql("select * from person")
    res0.show(false)

    val res1: DataFrame = spark.sql("select * from global_temp.person")
    res1.show(false)

    val res2 = spark.newSession().sql("select name,age from global_temp.person")
    res2.show(false)

    //以下代码会报错,原因：Spark SQL中的临时视图是会话范围的,如果创建它的会话终止,它将消失
    val res3 = spark.newSession().sql("select name,age from person")
    res3.show(false)

    spark.stop()
  }



}