package com.sparksql

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro._

/**
  * Created by root on 2019/8/4.
  */
class AvroDemo {

}

object AvroDemo{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[1]").getOrCreate()

    val usersDF: DataFrame = spark.read.format("avro").load("users.avro")
    usersDF.printSchema()
    usersDF.show(false)
    //usersDF.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")


    //未测试

    val jsonFormatSchema: String = new String(Files.readAllBytes(Paths.get("people.txt")))
    println(jsonFormatSchema)

//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("subscribe", "topic1")
//      .load()

    // 1. Decode the Avro data into a struct;
    // 2. Filter by column `favorite_color`;
    // 3. Encode the column `name` in Avro format.
//    val output = df
//      .select(from_avro('value, jsonFormatSchema) as 'user)
//      .where("user.favorite_color == \"red\"")
//      .select(to_avro($"user.name") as 'value)

//    val query = output
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("topic", "topic2")
//      .start()



  }

}