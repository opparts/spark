package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object simple {
  def main(args: Array[String]) {
    val logFile = "/Applications/spark-2.1.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val firstline = logData.first()
    println(s"first line is $firstline")
    sc.stop()
  }
}