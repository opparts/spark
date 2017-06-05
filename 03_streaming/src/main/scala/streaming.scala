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
    val ssc = new StreamingContext(conf, Seconds(10))
   
    val line =  ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK_SER_2);  //不设置storeagelevel，也是这个默认的值
    
    val errorline = line.filter(_.contains("err"));
    errorline.print();
    println("All error is" + errorline.count());
    
    ssc.start();
    ssc.awaitTermination();
    
    
    
  }
}