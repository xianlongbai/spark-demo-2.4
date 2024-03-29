package com.sparkstreaming.custom

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


/**
  * Created by root on 2019/9/10.
  * 自定义套接字接收文本流的接收器
  *
  *
  */
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  override def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        //这里涉及到到接收器的可靠性
          //不可靠的接收器：简单实施，系统负责块生成和速率控制。没有容错保证，可能会丢失接收器故障数据。
          //可靠的接收器：强大的容错保证，可确保零数据丢失。 块生成和速率控制由接收机实现处理。 实现的复杂程度取决于源的确认机制。
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}


//使用自定义的
object CustomReceiver {

  def main(args: Array[String]) {
    //参数个数判断
//    if (args.length < 2) {
//      System.err.println("Usage: CustomReceiver <hostname> <port>")
//      System.exit(1)
//    }
    // Create the context with a 1 second batch size
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // Create an input stream with the custom receiver on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    val lines = ssc.receiverStream(new CustomReceiver("localhost",9999))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
