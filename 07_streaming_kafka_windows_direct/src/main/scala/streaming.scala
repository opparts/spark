package main.scala

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder

object streaming {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Windows Version Streaming for Kafka")
    
   
    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    val ssc = new StreamingContext(conf, Seconds(5))
 
    ssc.checkpoint("/tmp/checkpoint")
    
    
    val topics = Set("test1")
    val brokers = "localhost:9092"
    val kafkaParams = Map("metadata.broker.list" -> brokers,
      "group.id" -> "anygroup")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    
    
    // Get the lines, split them into words, count the words and print
    val lines = kafkaStream.map(_._2)
    val key_count = lines.map(x => ( x.split(",")(1),1 ))
    val wordCounts = key_count.reduceByKey(_ + _)
    val result = key_count.reduceByKeyAndWindow((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10), 2)
    result.print()
    
    ssc.start()
    ssc.awaitTermination()

  }
}
