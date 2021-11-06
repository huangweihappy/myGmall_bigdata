package com.athw.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01 {
  def main(args: Array[String]): Unit = {

    // todo 获取配置文件，设置参数
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming")
//    todo 创建Streaming 对象 第一个参数是配置文件，第二个参数是隔多长时间读取一次数据
    val context: StreamingContext = new StreamingContext(conf,Seconds(3))
//    todo 创建监听端口的流对象，并监听 hadoop102 的 9999 端口数据
    val input: ReceiverInputDStream[String] = context.socketTextStream("hadoop102",9999)
//   todo wordCount
    input.flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_).print


// todo 开启流处理，并阻塞进程，每三秒计算一次
    context.start()
    context.awaitTermination()
  }

}
