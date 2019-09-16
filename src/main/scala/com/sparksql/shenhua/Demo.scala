package com.sparksql.shenhua

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2019/8/28.
  */

case class A(a: String, b: Int)
case class B(c: List[A], d: Map[String, A], e: Map[Int, String], f: Map[A, String])

class Demo {}

object Demo {

  def a_gen(i: Int) = A(s"str_$i", i)
  def b_gen(i: Int) = B((1 to 10).map(a_gen).toList, (1 to 10).map(j => s"key_$j" -> a_gen(j)).toMap, (1 to 10).map(j => j -> s"value_$j").toMap, (1 to 10).map(j => a_gen(j) -> s"value_$j").toMap)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = (1 to 10).map(b_gen)
    val df = spark.createDataFrame(data)
    df.printSchema
    df.show(10,false)

    //c的数据类型为array，我们可以单纯使用点的方式把数组中的某个结构给提取出来
    //同样可以使用expr("c['a']")或col("c")("a")的方式获得相同的结果。
    df.select("c.a").show(10, false)
    df.select("c.a").printSchema

    //这里介绍一个很有用的表达式explode，它能把数组中的元素展开成多行数据
    //比如：
    //> SELECT explode(array(10, 20));
    // 10
    // 20
    //还有一个比较有用的函数是posexplode，顾名思义，这个函数会增加一列原始数组的索引
    df.selectExpr("explode(c.a)").show(10)
    df.selectExpr("explode(c.a)").printSchema
    df.selectExpr("explode(c)").show(10)
    df.selectExpr("explode(c)").printSchema
    df.selectExpr("inline(c)").show(10)
    df.selectExpr("inline(c)").printSchema

    //操作Map
    df.selectExpr("posexplode(d)").printSchema
    df.selectExpr("posexplode(e)").printSchema
    df.selectExpr("posexplode(f)").show(10)
    df.selectExpr("posexplode(f)").printSchema

    //我们可以使用点表达式去用map的key取value
    //如果key不存在这行数据会为null
    df.select("d.key_1").show(10)

    //数字为key同样可以使用
    //对于数字来讲，expr("e[1]")、expr("e['1']")、col("e")(1)、col("e")("1")这四种表达式都可用
    //只是最后取得的列名不同
    df.select("e.1").show(10)

    //我们用struct作为map的key
    //这种情况下，我们可以用namedExpressionSeq表达式类构造这个struct
    //???
    df.selectExpr("f[('str_1' AS a, 1 AS b)]").show(10)
    df.selectExpr("f[('str_1' AS a, 1 AS b)]").printSchema()

  }


}
