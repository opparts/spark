package main.scala


import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object streaming {
  def main(args: Array[String]) {
    
    
    //目前只有local【2】才能收到消
    // 而且在另外一个console中，先启动nc -lk 9999 ,再提交这个job，就没有出现connection refuse的错误消息
    val conf = new SparkConf().setMaster("local[2]").setAppName("网络监听9999端口")
    val ssc = new StreamingContext(conf, Seconds(5))
    
    //在有状态的情况下，需要打开检查点机制来保存容错性
    //其实相当于需要在本地保存这些临时数据（前后批次的数据，我猜的话，但是还没有看checkpoint的函数说明，后续再修改这个注释
    ssc.checkpoint("/Users/apple/tmp/checkpoint")
    
    //监控一个本地的文件系统目录,创建一个DStreaming出来
    val logdata = ssc.textFileStream("/Users/apple/tmp/log_for_check")
    
    //val ipline = logdata.filter(_.contains("error"));
    
    //创建一个滑动窗口, 按5秒为一个批次，每次永远包含10秒内的所有批次（即2个批次）
    //这里仅仅使用window(),下面的其他的函数就不使用了。
    //error,shanghai,alex,2014-01-02,login 
    //取alex，这个列作为关键字，来算多少人登录了
    val ipwindow = logdata.window(Seconds(10),Seconds(5));
    val key_count = ipwindow.map( x =>  ( x.split(",")(1),1 ) )
    
    
    val after_aggregated_result = key_count.reduceByKey((x,y) => x+y );
    after_aggregated_result.print()

    after_aggregated_result.saveAsTextFiles("/Users/apple/tmp/streaming_output/result", "txt")
    
    
    ssc.start();
    ssc.awaitTermination();    

 
  }
}