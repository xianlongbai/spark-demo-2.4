package com.sparksql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by root on 2019/7/21.
  */

case  class  Person ( name:String, age:Long )

object DataSetDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    import spark.implicits._

    val caseClassDS: Dataset[Person] = Seq(Person("Andy", 32)).toDS()
    caseClassDS.printSchema()
    caseClassDS.show()

    val path = "people.txt"
    val peopleDS: Dataset[Person] = spark.read.json(path).as[Person]
    peopleDS.show()

    spark.stop()
  }

}
