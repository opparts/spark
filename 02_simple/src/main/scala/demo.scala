package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader


//引入了opencsv.jar文件在项目的工程中

object demo {
  def main(args: Array[String]) {
    val path = "file:///Users/apple/user.csv" // Should be some file on your system

    val conf = new SparkConf().setMaster("spark://apple.local:7077").setAppName("Reading CSV file and then Printing it")
    val sc = new SparkContext(conf)

    //创建一个名为logData的RDD对象
    val user = sc.textFile(path);
    val result = user.map {
      line =>
        val reader = new CSVReader(new StringReader(line));
        reader.readNext();
    };
    result.collect().foreach( x => { x.foreach(println);println("--------------")});
    sc.stop()
  }
}