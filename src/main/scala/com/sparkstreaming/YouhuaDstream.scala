package com.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2019/9/15.
  */
case class Student(name:String,age:Int){}

class YouhuaDstream {

  def main(args: Array[String]): Unit = {

    //这里至少得配置两个线程才能打印出相关数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("first")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true") //优雅的关闭程序
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //如果序列化对象很大，需要配置如下参数
    conf.set("spark.kryoserializer.buffer","64k")
    conf.set("spark.kryoserializer.buffer.max","64m")
    val ssc = new StreamingContext(conf,Seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //Spark 2.0.0版本开始，简单类型、简单类型数组、字符串类型的Shuffling RDDs 已经默认使用Kryo序列化方式了
    //对于一些自定义的类需要手动注册,可以一次性注册多个类
    //如果你没有注册你的自定义类，Kryo仍然会工作，但它必须存储每个对象的完整类名，这是浪费
    conf.registerKryoClasses(Array(classOf[Student]))

    //todo... 其它优化策略  内存管理、数据结构、GC、并行度、广播变量、数据位置

  }

}
