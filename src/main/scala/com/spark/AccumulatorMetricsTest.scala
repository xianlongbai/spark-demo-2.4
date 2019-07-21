package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.metrics.source.{DoubleAccumulatorSource, LongAccumulatorSource}

/**
  * Created by root on 2019/7/20.
  */
object AccumulatorMetricsTest {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
//      .config("spark.metrics.conf.*.sink.console.class",
//        "org.apache.spark.metrics.sink.ConsoleSink")
        .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val acc = sc.longAccumulator("my-long-metric")
    // register the accumulator, the metric system will report as
    // [spark.metrics.namespace].[execId|driver].AccumulatorSource.my-long-metric
//    LongAccumulatorSource.register(sc, List(("my-long-metric" -> acc)).toMap)

    val acc2 = sc.doubleAccumulator("my-double-metric")
    // register the accumulator, the metric system will report as
    // [spark.metrics.namespace].[execId|driver].AccumulatorSource.my-double-metric
//    DoubleAccumulatorSource.register(sc, List(("my-double-metric" -> acc2)).toMap)

    val num = if (args.length > 0) args(0).toInt else 1000000

    val startTime = System.nanoTime

    val accumulatorTest = sc.parallelize(1 to num).foreach(_ => {
      acc.add(1)
      acc2.add(1.1)
    })

    // Print a footer with test time and accumulator values
    println("Test took %.0f milliseconds".format((System.nanoTime - startTime) / 1E6))
    println("Accumulator values:")
    println("*** Long accumulator (my-long-metric): " + acc.value)
    println("*** Double accumulator (my-double-metric): " + acc2.value)

    spark.stop()
  }


}
