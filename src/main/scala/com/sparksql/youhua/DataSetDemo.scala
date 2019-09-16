package com.sparksql.youhua

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql._

/**
  * Created by root on 2019/8/9.
  */
class DataSetDemo {

}
case class hashstr(hash:String)
case class appinfo(tdid:String, applist:Seq[String])

object DataSetDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val devId = spark.read.textFile("tdid.txt").toDF("tdid")
    val tdid = broadcast(devId)

    val df02 = spark.read.parquet("test2").toDF("tdid", "applist")
    val df01: DataFrame = spark.read.parquet("test").toDF("tdid", "applist")
    val join01 = df01.join(tdid, Seq("tdid"), "right")
    val join02 = df02.join(tdid, Seq("tdid"), "right")

    val merge: DataFrame = join01.union(join02).toDF()
    //merge.show(false)
    //merge.printSchema()


    val res0: DataFrame = merge.select("tdid","applist")
    import spark.implicits._
//    val res1: Dataset[(String, Seq[String])] = res0.map(x => (x.getString(0),x.getSeq[String](1)) )
//    res1.show(false)
    val res2_1: Dataset[appinfo] = res0.as[appinfo]
    val res2_2: Dataset[(String, Seq[String])] = res2_1.map(x => (x.tdid,x.applist))
//    res2_2.show(false)
    val res2_3: KeyValueGroupedDataset[String, (String, Seq[String])] = res2_2.groupByKey(_._1)
    val res: Dataset[(String, (String, Seq[String]))] = res2_3.reduceGroups((a, b) => {
      var value: Seq[String] = Nil
      if ( a._2!=null && b._2!=null ) {
        value = a._2 ++ b._2
        (a._1,a._2 ++ b._2)
      } else if( a._2==null && b._2!=null ) {
        b._2.++(Nil)
        (b._1,b._2.++(Nil))
      } else if( b._2==null && a._2!=null ) {
        a._2.++(Nil)
        (a._1,a._2.++(Nil))
      } else {
        (a._1,value)
      }
    })

    res.printSchema()

//    val groupRdd: KeyValueGroupedDataset[String, (String, Seq[String])] = rdd.groupByKey(_._1)
//    val resRddd: Dataset[(String, (String, Seq[String]))] = groupRdd.reduceGroups((a, b) => {
//      (a._1, (a._2 ++ b._2).distinct)
//    })
//
//    resRddd.show(false)
    res.show(false)
    while (true){}
    spark.stop()
  }

}
