package com.athw.spark.req


import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Req_hotCategory10_2 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
  val sc = new SparkContext(conf)
  // todo 统计热点商品排名top10
  //  todo 1.获取原始数据
  val datas: RDD[String] = sc.textFile("data/user_visit_action.txt")

  val acc  =  new HotCategoryAccumulator()
  sc.register(acc,"hotCount")
  datas.foreach{
    str =>{
      val datas  = str.split("_")
      if(datas(6) != "-1"){
        acc.add((datas(6),"click"))
      }else if(datas(8) != "null"){
        datas(8).split(",").foreach(
          str =>{
            acc.add((str,"order"))
          }
        )

      }else if(datas(10) != "null"){
        datas(10).split(",").foreach(
          str =>{
            acc.add((str ,"pay"))
          }
        )
      }
    }

  }

  acc.value.map(_._2).toList.sortWith(
    (t1,t2) =>{
      if(t1.clickCount>t2.clickCount){
        true
      }else if(t1.clickCount == t2.clickCount){
        if(t1.orderCount> t2.orderCount){
          true
        }else if(t1.orderCount == t2.orderCount){
         t1.payCount >t2.payCount
        }else{
          false
        }
      }else{
        false
      }
    }
  ).take(10).foreach(println)
  sc.stop()

  case  class hotCategoryCount(cid:String,var clickCount:Int,var orderCount:Int,var payCount:Int)

  class HotCategoryAccumulator extends AccumulatorV2[(String ,String),mutable.Map[String,hotCategoryCount]]{
    private val map  =mutable.Map[String,hotCategoryCount]()
    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, hotCategoryCount]] ={
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: (String, String)): Unit = {
      val (cid,actionType) = v
      val categoryCount:hotCategoryCount = map.getOrElse(cid,hotCategoryCount(cid,0,0,0))
      actionType match {
        case "click" => categoryCount.clickCount += 1
        case  "order" => categoryCount.orderCount += 1
        case "pay" => categoryCount.payCount += 1
      }

      map.update(cid,categoryCount)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, hotCategoryCount]]): Unit = {
      other.value.foreach{
        case (cid,otherHcc) =>{
          val categoryCount = map.getOrElse(cid,hotCategoryCount(cid,0,0,0))
            categoryCount.clickCount += otherHcc.clickCount
            categoryCount.orderCount += otherHcc.orderCount
            categoryCount.payCount += otherHcc.payCount
          map.update(cid,categoryCount)


        }
      }
    }

    override def value: mutable.Map[String, hotCategoryCount] = {
      map
    }
  }


}