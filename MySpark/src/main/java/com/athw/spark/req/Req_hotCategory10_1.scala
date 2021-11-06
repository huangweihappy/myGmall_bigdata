package com.athw.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req_hotCategory10_1 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
  val sc = new SparkContext(conf)
  // todo 统计热点商品排名top10
  //  todo 1.获取原始数据
  val datas: RDD[String] = sc.textFile("data/user_visit_action.txt")
  //  todo 2.对数据进行封装
  datas.flatMap(
    str => {
      val datas = str.split("_")
      if (datas(6) != "-1") {
        List((datas(6), (1, 0, 0)))
      } else if (datas(8) != "null") {
        datas(8).split(",").map(
          st =>{
            (st,(0,1,0))
          }

        )
      }else if (datas(10) != "null"){
            datas(10).split(",").map(
        sts =>{

          (sts,(0,0,1))
        }
     )
      }else{
        Nil
      }
    }
        ).reduceByKey(
    (t1,t2) =>{
      (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
    }
  ).sortBy(_._2,false).take(10).foreach(println)

  //  todo 3.对数据进行排序（点击，下单，排序）



  //  todo 4. 取top10 打印在控制台


}