package com.athw.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestReq1 extends App{




  val conf =   new SparkConf().setMaster("local[*]").setAppName("Req")
  val sc =  new SparkContext(conf)
  // todo 统计出每一个省份每个广告被点击数量排行的Top3
      val lines: RDD[String] = sc.textFile("data/agent.log")
  lines.map(_.split(" ")).map(
    str => {
      ((str(1), str(4)), 1)
    }
  ).groupByKey().mapValues(_.size).map(str =>{
    (str._1._1,(str._1._2,str._2))
  }).groupByKey().mapValues(
    list =>{
      list.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }
  ).collect().foreach(println)



}
