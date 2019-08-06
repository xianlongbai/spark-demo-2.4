package com.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by root on 2019/7/22.
  *
  * hive schema 和 parquet schema 的区别
  * 1、Hive is case insensitive, while Parquet is not
  * 2、Hive considers all columns nullable, while nullability in Parquet is significant
  */
class LoadDemo {

}

object LoadDemo{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val usersDF = spark.read.load("D:\\tmp\\spark_sql\\parquet")
    usersDF.printSchema()
    usersDF.select("name").show(false)


//    val peopleDF = spark.read.format("json").load("people.txt")
//    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    //读取csv
//    val peopleDFCsv: DataFrame = spark.read.format("csv")
//      .option("sep", "|")
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .load("StudentData.csv")
//    peopleDFCsv.printSchema()
//    peopleDFCsv.show(false)

    //写入orc格式文件
//    peopleDFCsv.write.format("orc")
//      .option("orc.bloom.filter.columns", "studentName")
//      .option("orc.dictionary.key.threshold", "1.0")
//      .save("users_with_options.orc")

//    val orcDf: DataFrame = spark.read.format("orc").load("users_with_options.orc")
//    orcDf.printSchema()
//    orcDf.show(false)

    //可以使用SQL直接查询该文件，而不是使用read API将文件加载到DataFrame并查询它
    val sqlDF = spark.sql("SELECT * FROM parquet.`namesAndAges.parquet`")
    sqlDF.show()
    val sqlDF2 = spark.sql("SELECT * FROM orc.`users_with_options.orc`")
    sqlDF2.show()


    //以id分区保存为table
    sqlDF2
      .write
      .partitionBy("id")
      //.bucketBy(2, "name")  //表示分桶数为2,分桶字段为name
      .saveAsTable("student_info")

  }

}
