package com.athw.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Req_hotCategory10  extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
  val sc = new SparkContext(conf)
  // todo 统计热点商品排名top10
  //  todo 1.获取原始数据
  val datas: RDD[String] = sc.textFile("data/user_visit_action.txt")





sc.stop()
}
case class CategoryCount(ids :String,var clickCount :Int,var orderCount :Int,var payCount :Int )


class MyAcc extends AccumulatorV2[(String,String),mutable.Map[String,CategoryCount]] {
  private val map = mutable.Map[String,CategoryCount]()
  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, CategoryCount]] = {
    new MyAcc()
  }

  override def reset(): Unit ={
    map.clear()
  }

  override def add(v: (String, String)): Unit = {

    val (ids,categoryCountTypes) = v
    val hotCount = map.getOrElse(ids,CategoryCount(ids,0,0,0))

    categoryCountTypes match {
      case "click" => hotCount.clickCount += 1
      case "order" => hotCount.orderCount += 1
      case "pay" => hotCount.payCount += 1
    }
    map.update(ids,hotCount)
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, CategoryCount]]): Unit ={
    other.value.foreach{
      case ( cid, otherHCC ) => {
        val thisHCC: CategoryCount = map.getOrElse(cid, CategoryCount(cid, 0, 0, 0))
        thisHCC.clickCount += otherHCC.clickCount
        thisHCC.orderCount += otherHCC.orderCount
        thisHCC.payCount += otherHCC.payCount
        map.update(cid, thisHCC)
      }

    }

  }

  override def value: mutable.Map[String, CategoryCount] ={
    map
  }
}