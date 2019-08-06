package com.sparksql.udaf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by root on 2019/7/21.
  */
class TestUdaf {

}

object TestUdaf{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

//    val pDf: DataFrame = spark.read.json("people.txt")
//    pDf.createTempView("people")
//
//    spark.udf.register("myAverage", MyAverage)
//
//    val result = spark.sql("SELECT myAverage(age) as ave_age FROM people")
//    result.show()
    import spark.implicits._
    val eDs: Dataset[Employee] = spark.read.json("employees").as[Employee]
    eDs.show()

    //将函数转换为'TypedColumn'并给它一个名称
    val averageSalary = MyAverageTwo.toColumn.name("average_salary")
    val res = eDs.select(averageSalary)
    res.show()

  }


}
