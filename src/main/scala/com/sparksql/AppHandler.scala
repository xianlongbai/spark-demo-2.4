package com.sparksql


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * Created by root on 2019/7/27.
  */

case class App(
                tdid: String,
                applist: Seq[Long]
              )

class AppHandler {}

object AppHandler{


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    import spark.implicits._


    val df01 = spark.read.parquet("test").toDF("tdid", "applist")
    val df02 = spark.read.parquet("test2").toDF("tdid", "applist")
    val tdid = spark.read.textFile("tdid.txt").toDF("tdid")
    val join01 = df01.join(tdid, Seq("tdid"), "right")
    val join02 = df02.join(tdid, Seq("tdid"), "right")
    val df: Dataset[Row] = join01.union(join02)
    //df.show(false)

    val changeRdd: RDD[(String, Seq[String])] = df.rdd.map(x => (x.getString(0),x.getSeq[String](1)))
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

  }

}
