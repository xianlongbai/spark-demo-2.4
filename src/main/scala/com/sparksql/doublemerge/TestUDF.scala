package com.sparksql.doublemerge

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by root on 2019/8/8.
  */
object TestDoubleMerge {

  def randomPrefix(name: String,num:Int): String = {
    var random = (new util.Random).nextInt(num)
    random+"_"+name
  }

  def removeRandomPrefix(name: String): String = {
    var split = name.split("_")
    split(1)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("xxx")
        .master("local")
        .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("debug")

    //java自定义udf函数
//    spark.udf.register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType)
//    spark.udf.register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType)
    //scala自定义udf函数
    spark.udf.register("random_prefix", (name:String,num: Int)=>randomPrefix(name,num))
    spark.udf.register("remove_random_prefix", (name:String)=>removeRandomPrefix(name))

    val personRDD =
      sc.parallelize(List("A", "A", "A", "A", "B"), 1)
        .map(x => (x, 1))
        .map(x => Row(x._1, x._2.toInt))

    // 创建Schema
    val schema: StructType = StructType(Seq(
      StructField("product", StringType, false),
      StructField("click", IntegerType, false)
    ))

    val personDF = spark.createDataFrame(personRDD, schema)

    //SQL语法操作
    personDF.createOrReplaceTempView("t_product_click")

    // 加随机前缀
    val sql1 =
      s"""
         |select
         |  random_prefix(product, 2) product,
         |  click
         |from
         |  t_product_click
       """.stripMargin

    // 分组求和
    val sql2 =
      s"""
         |select
         |  product,
         |  sum(click) click
         |from
         |  (
         |    select
         |      random_prefix(product, 2) product,
         |      click
         |    from
         |      t_product_click
         |  ) t1
         |group by
         |  product
       """.stripMargin

    // 去掉随机前缀
    val sql3 =
      s"""
         |select
         |  remove_random_prefix(product) product,
         |  click
         |from
         |  (
         |    select
         |      product,
         |      sum(click) click
         |    from
         |      (
         |        select
         |          random_prefix(product, 2) product,
         |          click
         |        from
         |          t_product_click
         |      ) t1
         |    group by
         |      product
         |  ) t2
         |
       """.stripMargin

    // 分组求和
    val sql4 =
      s"""
         |select
         |  product,
         |  sum(click) click
         |from
         |  (
         |    select
         |      remove_random_prefix(product) product,
         |      click
         |    from
         |      (
         |        select
         |          product,
         |          sum(click) click
         |        from
         |          (
         |            select
         |              random_prefix(product, 2) product,
         |              click
         |            from
         |              t_product_click
         |          ) t1
         |        group by
         |          product
         |      ) t2
         |  ) t3
         |group by
         |  product
       """.stripMargin

    import spark.sql
//    sql(sql1).show()
//    sql(sql2).show()
    sql(sql3).show()
//    sql(sql4).show()
    sc.stop()


  }

}
