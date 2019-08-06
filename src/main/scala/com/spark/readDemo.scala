package com.spark

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by root on 2019/7/20.
  */
class readDemo {

  def func2(s: String): String = {
    s.substring(128)
  }

}

object MyFunctions {
  def func1(s: String): String = {
    s.substring(128)
  }
}

object readDemo{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[4]").getOrCreate()

    //textFile支持在目录，压缩文件和通配符上运行
    val textRdd: Dataset[String] = spark.read.textFile("data.csv.gz")
    //textRdd.take(2).foreach(println)

//    Spark的API在很大程度上依赖于在驱动程序中传递函数以在集群上运行。有两种建议的方法可以做到这一点：
//    匿名函数语法，可用于短代码。
//    全局单例对象中的静态方法。

    textRdd.take(2).map(MyFunctions.func1).foreach(println)
//    val demoClazz = new readDemo()
//    textRdd.take(2).map(demoClazz.func2).foreach(println)

  }


}