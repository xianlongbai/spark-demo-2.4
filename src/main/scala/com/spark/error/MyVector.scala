package com.spark.error

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/**
  * Created by root on 2019/7/21.
  */
//class MyVector(x:Int,y:Int) {
//
//  val x = 0
//  val y = 0
//
//  def createZeroVector(): MyVector = {
//    new MyVector(0,0)
//  }
//
//  def merge(other: AccumulatorV2[MyVector, MyVector]): Unit = {
//    println(111)
//  }
//
//
//  def isZero(): Boolean = {
//    if (this.x==0 && y ==0){
//      true
//    } else {
//      false
//    }
//  }
//
//
//  def copy():AccumulatorV2[MyVector,MyVector] = {
//    //创建新的累加器
//    val newVectorAccumulatorV2 = new VectorAccumulatorV2()
//    val newMyVector = new MyVector(x,y)
//    //初始化累加器
//    newVectorAccumulatorV2.add(newMyVector)
//    newVectorAccumulatorV2
//  }
//
//
//  def add(v: MyVector): Unit = {
//    MyVector = MyVector  v
//    MyVector.y = MyVector.y + v.y
//  }
//
//
//  def reset(x:Int,y:Int): Unit ={
//    x = x
//    this.y = y
//  }
//
//
//}

object MyVector {






  def main(args: Array[String]): Unit = {

//    def main(args: Array[String]) {
//
//      val spark = SparkSession
//        .builder()
//        .master("local")
//        .getOrCreate()
//      val sc = spark.sparkContext
//
//
//      val myVectorAcc = new VectorAccumulatorV2
//      sc.register(myVectorAcc, "MyVectorAcc1")
//
//      val accumulatorTest = sc.parallelize(1 to 30).foreach(_ => {
//        myVectorAcc.add(new MyVector(1, 1))
//      })
//
//      println(myVectorAcc.value)
//
//    }


  }
}