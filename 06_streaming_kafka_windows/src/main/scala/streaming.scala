package main.scala

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark


object streaming {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Windows Version Streaming for Kafka")
    
   
    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    val ssc = new StreamingContext(conf, Seconds(5))
 
    ssc.checkpoint("/tmp/checkpoint")

    val zkQuorum = "localhost:2181"
    val groupid = "anygroup" 
    val topicMap = "test1".split(":").map((_, 1)).toMap 
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupid, topicMap,StorageLevel.MEMORY_AND_DISK_SER)

   

    val lines = kafkaStream.map(_._2)
    
    val key_count = lines.map(x => ( x.split(",")(1),1 ))
    val result = key_count.reduceByKeyAndWindow((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10), 2)
    result.print()
    
    ssc.start()
    ssc.awaitTermination()

  }
}
