package com.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by root on 2019/7/25.
  */
class JdbcDfDemo {

}
object JdbcDfDemo{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local")
      .getOrCreate()

    val jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/spider?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&rewriteBatchedStatements=true&useSSL=false&socketTimeout=30000&connectTimeout=3000&serverTimezone=GMT%2B8")
      .option("dbtable", "goods_info")
      .option("user", "root")
      .option("password", "root")
      .load()

//    jdbcDF.printSchema()
//    jdbcDF.show(10,false)

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")
    val jdbcDF2: DataFrame = spark.read
      .jdbc("jdbc:mysql://localhost:3306/spider?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&rewriteBatchedStatements=true&useSSL=false&socketTimeout=30000&connectTimeout=3000&serverTimezone=GMT%2B8",
        "test_goods", connectionProperties)
    jdbcDF2.printSchema()
    jdbcDF2.show()


    //写库(必须指定库中不存在的表名)
    jdbcDF2.select("price","name").write
      //.option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:mysql://localhost:3306/spider?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&rewriteBatchedStatements=true&useSSL=false&socketTimeout=30000&connectTimeout=3000&serverTimezone=GMT%2B8",
        "tmp_001", connectionProperties)



  }

}