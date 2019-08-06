package com.spark.error

import org.apache.spark.util.AccumulatorV2

/**
  * Created by root on 2019/7/21.
  */
//class VectorAccumulatorV2 extends AccumulatorV2[MyVector, MyVector]{
//
//  private val myVector: MyVector = MyVector.createZeroVector()
//
//  //返回该累加器是否为零值。例如，对于计数器累加器，0是零值;对于列表累加器，Nil是零值。
//  override def isZero: Boolean = {
//    myVector.isZero()
//  }
//
//  //创建此累加器的新副本
//  override def copy(): AccumulatorV2[MyVector, MyVector] = {
//    myVector.copy()
//  }
//
//  //重置这个累加器，它是零值。
//  override def reset(): Unit = {
//    myVector.reset(0,0);
//  }
//
//  //接受输入并累加。
//  override def add(v: MyVector): Unit = {
//    myVector.add(v)
//  }
//
//  //将另一个相同类型的累加器合并到此累加器中并更新其状态，即该累加器应该是就地合并的。
//  override def merge(other: AccumulatorV2[MyVector, MyVector]): Unit = {
//    myVector.merge(other)
//  }
//
//  //定义此累加器的当前值
//  override def value: MyVector = {
//    myVector
//  }
//
//
//}
