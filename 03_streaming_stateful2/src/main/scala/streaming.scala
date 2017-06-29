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
    
    ssc.checkpoint("/Users/apple/tmp")
    
    //line就是一个dstream对象
    val line =  ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK_SER_2);  //不设置storeagelevel，也是这个默认的值
    
    //对其进行了转化，但是依然还是DSTREAM对象，只拿当前行中有ip的这一个RDD元素
    //例如： http  ，ip:10.1.1.1,  alex    
    //例如： ftp   ，ip:10.1.1.1,  alex
    //例如： http  ，ip:10.1.1.1,  gordon
    //例如： http  ，ip:10.1.1.1,  simon 
    val ipline = line.filter(_.contains("ip"));
    
    //创建一个滑动窗口, 按5秒为一个批次，每次永远包含10秒内的所有批次（即2个批次）
    //这里仅仅使用window(),下面的其他的函数就不使用了。
    val key_count = ipline.map( x =>  ( x.split(",")(1),1 ) ).reduceByKeyAndWindow((x:Int,y:Int) =>(x + y) , Seconds(10) ,Seconds(5))
    key_count.print()
    
    ssc.start();
    ssc.awaitTermination();
    
    
    
  }
}