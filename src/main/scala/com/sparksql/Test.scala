package com.sparksql

/**
  * Created by root on 2019/8/5.
  */
object Test {

  def main(args: Array[String]): Unit = {

//    val str = "aaa,bbb,ccc,ddd,eee"
//    println(str.indexOf(","))

    val feature = "1234,aaa,bbb,ccc,ddd"

    val firstSeparatorInx = feature.indexOf(",")
    val tdid = feature.substring(0, firstSeparatorInx )
    val dimensions = feature.substring(firstSeparatorInx + 1)

    println(tdid)
    println(dimensions)

  }


}
