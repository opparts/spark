name := "topic1 - Windows Version Kafka"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-streaming_2.11"  % "2.1.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11"  % "2.1.0",
  "com.yammer.metrics" % "metrics-core" % "2.2.0"
)	

