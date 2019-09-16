package com.sparkstreaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 总结：（目前为实验版）
  * 新的spark-streaming-kafka-0-10客户端采用了与原有版本完全不同的架构，一个job里面运行了两组consumer：
  * driver consumer和 executor consumer，driver端consumer负责分配和提交offset到初始化好的KafkaRDD当中去，
  * KafkaRDD内部会根据分配到的每个topic的每个partition初始化一个CachedKafkaConsumer客户端通过assgin的方式
  * 订阅到topic拉取数据。
  */
object direct_kafka {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))

    sc.setCheckpointDir("D:\\tmp\\sparkstreaming\\checkpoint-005")

//过期的
//     val topics = Map("spark" -> 2)
//    val kafkaParams = Map[String, String](
//      "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092",
//      "group.id" -> "spark_direct",
//      "auto.offset.reset" -> "smallest"
//    )
//    kafka 0.8 连接方式,spark2.3之后就不支持了
//    val lines =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test01")).map(_._2)


    //配置kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_direct",
      "auto.offset.reset" -> "earliest", //读取位置： latest, earliest, none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test")

    //在Kafka中记录读取偏移量
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略（可用的Executor上均匀分配分区）
      LocationStrategies.PreferConsistent,
      //消费策略（订阅固定的主题集合）,还可以指定第三个参数：指定特定分区的起始偏移量
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //注意：如果采用以下官方文档手动提交offset，需要把stream对象的属性标记为static或者transient避免序列化，
    // 不然可能在任务提交的时候报DirectKafkaInputDStream 无法序列化导致Task not serializable错误
    stream.foreachRDD { rdd =>
      //获取该RDD对于的偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //拿出对应的数据
      rdd.foreach{ line =>
        println(line.key() + " " + line.value())
      }
      //异步更新偏移量到kafka中
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
