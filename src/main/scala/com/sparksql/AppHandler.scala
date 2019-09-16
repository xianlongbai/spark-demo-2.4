package com.sparksql


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * Created by root on 2019/7/27.
  *
  * 执行计划4个阶段：(类似一棵树，从下往上看)
  *   1、解析过程
  *   2、逻辑阶段
  *   3、优化阶段
  *   4、物理执行计划
  */

case class App(
                tdid: String,
                applist: Seq[Long]
              )

class AppHandler {}

object AppHandler{


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    import spark.implicits._


    val df01 = spark.read.parquet("test").toDF("tdid", "applist")
    val df02 = spark.read.parquet("test2").toDF("tdid", "applist")
    val tdid = spark.read.textFile("tdid.txt").toDF("tdid")
    val tdidModel = spark.read.textFile("tdid.txt").map((_,null)).toDF("tdid","applist")
    tdidModel.printSchema()
    tdidModel.show(false)
    println("--------------------------------------------------")
//    val tdid = broadcast(devId)
    val join01 = df01.join(broadcast(tdid), Seq("tdid"), "inner")
    join01.explain()
    val join02 = df02.join(broadcast(tdid), Seq("tdid"), "inner")
    join02.explain()
    val df: Dataset[Row] = join01.union(join02)
    df.show(false)
    println("--------------------------------------------------")
    val df2 = df.union(tdidModel)
    df2.show(false)

    val changeRdd: RDD[(String, Seq[String])] = df2.rdd.map(x => (x.getString(0),x.getSeq[String](1)))
    //val groupRdd: RDD[(String, Seq[String])] = changeRdd.reduceByKey(_.++(_))
    val groupRdd: RDD[(String, Seq[String])] = changeRdd.reduceByKey((x: Seq[String], y: Seq[String]) => {
      if ( x!=null && y!=null ) x.++(y)
      else if( x==null && y!=null ) y.++(Nil)
      else if( y==null && x!=null ) x.++(Nil)
      else Nil
    })

    //1、
    val resDf: DataFrame = groupRdd.map(x => (x._1,x._2.distinct)).toDF("tdid","feature")
    //2、
    //val resDf: DataFrame = groupRdd.map(x => (x._1,x._2.distinct)).map(x => App(x._1,x._2)).toDF




    resDf.printSchema()
    resDf.show(false)

    while (true){}
  }

}
