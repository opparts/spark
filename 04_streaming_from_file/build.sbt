name := "测试Spark Streaming的测试程序 - 监控文件系统的目录，作为数据源"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-streaming_2.11"  % "2.1.0"
)