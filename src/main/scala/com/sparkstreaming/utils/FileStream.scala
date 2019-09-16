package com.sparkstreaming.utils

import java.io.{File, FileWriter}

/**
  * Created by root on 2019/9/9.
  *
  *
  */
class FileStream {

}

object FileStream {

  def main(args: Array[String]): Unit = {

//    val writer = new PrintWriter(new File("D:\\tmp\\sparkstreaming\\jiankong\\dometxt" ))
    val writer = new FileWriter(new File("D:\\tmp\\sparkstreaming\\jiankong\\dome1.txt" ),true)
    for (x <- 1 to 10){
      writer.write("yanzi hello shiwang love \n")
      writer.write("hello bxl bxl love \n")
      writer.write("hello hello love yanzi \n")
      Thread.sleep(5000)
      writer.flush()
    }
    writer.close()

  }

}