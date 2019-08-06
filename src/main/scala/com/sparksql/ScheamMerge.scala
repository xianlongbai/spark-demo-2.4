package com.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2019/7/22.
  */
class ScheamMerge {

}

object ScheamMerge{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
//    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
//    squaresDF.write.parquet("data_parttion/test_table/key=1")

//    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
//    cubesDF.write.parquet("data_parttion/test_table/key=2")

    //合并schema
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data_parttion/test_table")
    mergedDF.printSchema()
    mergedDF.show(false)


  }


}