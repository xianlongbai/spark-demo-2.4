name := "spark-demo-2.4"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"

//provided 表示在打包的时候不包含在内
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"
