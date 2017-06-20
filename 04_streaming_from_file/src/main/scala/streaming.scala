package main.scala


import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object streaming {
  def main(args: Array[String]) {
    
    
    //目前只有
    // 提交到本地
    val conf = new SparkConf().setMaster("local[2]").setAppName("监听本地MacOS的文件系统是否有改动")
    val ssc = new StreamingContext(conf, Seconds(10))
    
    //在有状态的情况下，需要打开检查点机制来保存容错性
    //其实相当于需要在本地保存这些临时数据（前后批次的数据，我猜的话，但是还没有看checkpoint的函数说明，后续再修改这个注释
    ssc.checkpoint("/Users/apple/tmp/checkpoint")
    
    //监控一个本地的文件系统目录,创建一个DStreaming出来
    val lines = ssc.textFileStream("/Users/apple/tmp/file")
    
    val words = lines.flatMap(_.split(","))
    
    

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //val key_count = words.map( x =>  ( x.split(",")(1),1 ) )
    //val after_aggregated_result = key_count.reduceByKey((x,y) => x+y );

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

 
  }
}