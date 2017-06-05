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
    
    //在有状态的情况下，需要打开检查点机制来保存容错性
    //其实相当于需要在本地保存这些临时数据（前后批次的数据，我猜的话，但是还没有看checkpoint的函数说明，后续再修改这个注释
    
    ssc.checkpoint("/Users/apple/tmp")
    
    //line就是一个dstream对象
    val line =  ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK_SER_2);  //不设置storeagelevel，也是这个默认的值
    
    //对其进行了转化，但是依然还是DSTREAM对象，只拿当前行中有ip的这一个RDD元素
    //例如： http  ，ip:10.1.1.1,  alex    
    //例如： ftp   ，ip:10.1.1.1,  alex
    //例如： http  ，ip:10.1.1.1,  gordon
    //例如： http  ，ip:10.1.1.1,  simon 
    val ipline = line.filter(_.contains("ip"));
    
    //创建一个滑动窗口, 按5秒为一个批次，每次永远包含15秒内的所有批次（即3个批次）
    val ipwindow = ipline.window(Seconds(15),Seconds(5));
    
    val count = ipwindow.count();
    println("Total batch is " + count);
    
    //按照csv，例如“，”逗号进行分割，然后只截取包含第2列, 并且放个计数器1，方便后面做聚合
    val result = ipwindow.map( x =>  ( x.split(",")(1),1 ) )
    val aggreated_access = result.reduceByKeyAndWindow(
        {(x,y) => x + y},
        {(x,y) => x - y},
        Seconds(15),
        Seconds(5)
    )
    
    aggreated_access.foreachRDD(rdd => rdd.collect().foreach(println))
    
    
    ssc.start();
    ssc.awaitTermination();
    
    
    
  }
}