name := "spark-demo-2.4"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-avro_2.11" % "2.4.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"

//目前实验版
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.0"

//生产环境
//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.11" % "2.4.1"

//provided 表示在打包的时候不包含在内
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"
