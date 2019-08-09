package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2019/8/6.
  *
  * 基于rdd的双重聚合
  */
class DoubleMerge {
}

object DoubleMerge{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    val array01 = Array(("aaa",1),("aaa",1),("aaa",1),("aaa",1),
                        ("aaa",1),("aaa",1),("aaa",1),("aaa",1),
                        ("bbb",1),("bbb",1))

    val initRdd: RDD[(String, Int)] = sc.makeRDD(array01)
    initRdd.map(x => {
      var random = (new util.Random).nextInt(2)
      (random+"_"+x._1,x._2)
    }).reduceByKey(_+_,2).map(x => {
      var num = x._1.indexOf("_")
      (x._1.substring(num+1),x._2)
    }).reduceByKey(_+_,1).foreach(println)




  }

}
