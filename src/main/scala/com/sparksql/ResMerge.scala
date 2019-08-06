package com.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2019/7/25.
  */
class ResMerge {

  def merge(resPaths: Array[String], outPath: String): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    var data: RDD[(String, String)] = null
    val separator = ","
    resPaths.foreach(path => {
      if (data == null) {
        data = sc.textFile(path).map(r=>{
          val firstSeparatorInx = r.indexOf(",")
          val tdid = r.substring(0, firstSeparatorInx - 1)
          val dimensions = r.substring(firstSeparatorInx + 1)
          tdid -> dimensions
        })
      } else {
        val tmpData = sc.textFile(path).map(r=>{
          val firstSeparatorInx = r.indexOf(",")
          val tdid = r.substring(0, firstSeparatorInx - 1)
          val dimensions = r.substring(firstSeparatorInx + 1)
          tdid -> dimensions
        })
        data = data.join(tmpData).map(r => {
          r._1 -> s"${r._2._1},${r._2._2}"
        })
      }
    })
    //data.foreach(println)
    data.map(r=>s"${r._1},${r._2}").repartition(1).saveAsTextFile(outPath);
  }


}

object ResMerge{

  def main(args: Array[String]): Unit = {

    //val Array(inputPath,outputPath) = args
    val resMerge = new ResMerge
//    val inputArray: Array[String] = inputPath.split(",")
    val inputArray = Array[String]("data3.csv.gz","data2.csv.gz")
    val outputPath = "D:\\tmp\\spark_sql\\tar_gz\\merge";
    resMerge.merge(inputArray,outputPath)

  }

}