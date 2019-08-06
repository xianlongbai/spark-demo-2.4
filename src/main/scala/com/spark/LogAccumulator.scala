package com.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
  * Created by root on 2019/7/21.
  * 自定义累加器的使用
  */
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]]{

  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
    //使用了类型匹配
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }

  }

  override def value: java.util.Set[String] = {
    //转为不可修改的集合
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

}


object test{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val accum: LogAccumulator = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum: Int = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag: Boolean = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    println(accum.value)

    while (true){}
    sc.stop()
  }


}