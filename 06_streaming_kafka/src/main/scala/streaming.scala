package main.scala

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark



// ---------------------基于Reveiver方式--------------------
// --------------------------------------------------------
//http://www.jianshu.com/p/8603ba4be007

object streaming {
  def main(args: Array[String]) {
     //目前只有
    // 创建一个配置 - 通用的
    val conf = new SparkConf().setMaster("local[2]").setAppName("监听Kafka的Topic为test的消息 - 基于Receiver方式")
    
    //这个是比较推荐的方式
    //使用WAL的方式，接受到的数据会被持久化到日志中，要开启storage level
    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    val ssc = new StreamingContext(conf, Seconds(10))
    
    //Streaming标准的检查点机制
    //在有状态的情况下，需要打开检查点机制来保存容错性
    //其实相当于需要在本地保存这些临时数据（前后批次的数据，我猜的话，但是还没有看checkpoint的函数说明，后续再修改这个注释
    ssc.checkpoint("/Users/apple/tmp/checkpoint")
    
    //创建一个Kafka的Receiver的流
    //group貌似是随便设置的，和topic无关，这个topic就叫topic
    val zkQuorum = "localhost:2181"
    val groupid = "anygroup" 
    val topicMap = "alex_topic".split(":").map((_, 1)).toMap 
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupid, topicMap,StorageLevel.MEMORY_AND_DISK_SER)

    
    //map(_._1)  数据是空的 null
    //map(_._2)  才是Kafka里面打入的数据  
    val lines = kafkaStream.map(_._2)
    
    val error_information = lines.filter(_.contains("error"))
    
    val ipwindow = error_information.window(Seconds(20),Seconds(10))
    //按照用户名称（CSV文件的第二列）来聚合
    // 文件流中的一行     error,alex,shanghai,2014-08-25,create SO
    val key_count = ipwindow.map( x =>  ( x.split(",")(1),1 ) )
    val after_aggregated_result = key_count.reduceByKey((x,y) => x+y )
    //Console里面顺便打印出来一下
    after_aggregated_result.print()
    //保存结果在本地
    after_aggregated_result.saveAsTextFiles("/Users/apple/tmp/output/job-output", "txt")
    
    ssc.start()
    ssc.awaitTermination()

  }
}

//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Duration, StreamingContext}
// 
///**
//  * 
//  */
//object streaming {
//  def main(args: Array[String]) {
// 
//    val conf = new SparkConf().setMaster("local[2]").setAppName("监听Kafka的Topic为test的消息 - 基于Receiver方式")
//    
//    //这个是比较推荐的方式
//    //使用WAL的方式，接受到的数据会被持久化到日志中，要开启storage level
//    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
//    val ssc = new StreamingContext(conf, Seconds(5))
//    
//    //Streaming标准的检查点机制
//    //在有状态的情况下，需要打开检查点机制来保存容错性
//    //其实相当于需要在本地保存这些临时数据（前后批次的数据，我猜的话，但是还没有看checkpoint的函数说明，后续再修改这个注释
//    ssc.checkpoint("/Users/apple/tmp/checkpoint")
//    
//    
//    val topics = Set("alex_topic") //我们需要消费的kafka数据的topic
//    val brokers = "localhost:9092"
//    val kafkaParam = Map[String, String](
////      "zookeeper.connect" -> "192.168.21.181:2181",
////      "group.id" -> "test-consumer-group",
//      "metadata.broker.list" -> brokers,// kafka的broker list地址
//      "serializer.class" -> "kafka.serializer.StringEncoder"
//    )
// 
//    val stream: InputDStream[(String, String)] = createStream(ssc, kafkaParam, topics)
// 
//    stream.map(_._2)      // 取出value
//      .flatMap(_.split(" ")) // 将字符串使用空格分隔
//      .map(r => (r, 1))      // 每个单词映射成一个pair
//      .updateStateByKey[Int](updateFunc)  // 用当前batch的数据区更新已有的数据
//      .print() // 打印前10个数据
//      
//      
//    ssc.start() // 真正启动程序
//    ssc.awaitTermination() //阻塞等待
//  }
//  
//  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
//    val curr = currentValues.sum
//    val pre = preValue.getOrElse(0)
//    Some(curr + pre)
//  }
//  /**
//    * 创建一个从kafka获取数据的流.
//    * @param scc           spark streaming上下文
//    * @param kafkaParam    kafka相关配置
//    * @param topics        需要消费的topic集合
//    * @return
//    */
//  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
//    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
//  }
//}
