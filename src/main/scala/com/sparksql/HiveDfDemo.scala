package com.sparksql

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by root on 2019/7/23.
  */

case class Record(key: Int, value: String)

class HiveDfDemo {}

object HiveDfDemo{

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    println("hive数仓的位置路径：" + warehouseLocation)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    //默认以纯文本的形式读取
    //sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) row format delimited fields terminated by ','")
//    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("create  table if not exists default.person (age int,name string) row format delimited fields terminated by ',' stored  as textfile")
    sql("LOAD DATA LOCAL INPATH 'kv2.txt' INTO TABLE default.person")

    val sqlDF: DataFrame = sql("SELECT * FROM default.person")

    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    val res: DataFrame = spark.table("person")
    res.show(false)



  }

}
