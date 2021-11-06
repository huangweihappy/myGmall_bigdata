package com.athw.spark.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Req_hotCategory10_3  extends  App {



  val conf = new SparkConf().setMaster("local[*]").setAppName("Top10")
  val sc = new SparkContext(conf)
  // todo 统计热点商品排名top10
  //  todo 1.获取原始数据
  val datas = sc.textFile("data/user_visit_action.txt")

 val acc = new myCateoryAcc()
  sc.register(acc,"categoryAcc")
  datas.foreach(
    str =>{
      val datas = str.split("_")
      if (datas(6) != "-1"){
        acc.add((datas(6),"click"))
      }else if(datas(8) != "null"){
        datas(8).split(",").foreach(
          str => {
            acc.add((str,"order"))
          }
        )
      }else if(datas(10) != "null"){
          datas(10).split(",").foreach(
            str => {
              acc.add((str,"pay"))
            }
          )
      }
    }
    )

  val dataCount: List[HotCategoryCount] = acc.value.map(_._2).toList

  dataCount.sortWith(
    (t1,t2) =>{
      if (t1.clickCount > t2.clickCount){
       true
      }else if(t1.clickCount == t2.clickCount){
        if(t1.orderCount >t2.orderCount){
          true
        }else if(t1.orderCount == t2.orderCount){
          t1.payCount > t2.payCount
        }else{
          false
        }
      }else {
        false
      }
    }
  ).take(10).foreach(println)

  sc.stop()
}
  case class HotCategoryCount(cid :String,var clickCount: Int ,var orderCount : Int ,var payCount : Int)

  class myCateoryAcc extends  AccumulatorV2[(String,String),mutable.Map[String,HotCategoryCount]]{
   private val map  = mutable.Map[String,HotCategoryCount]()
    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]] = {
      new myCateoryAcc()
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: (String, String)): Unit ={
      val (cid,actionType) =  v
      val hotCount =  map.getOrElse(cid,HotCategoryCount(cid,0,0,0))
        actionType match {
          case "click" => hotCount.clickCount += 1
          case "order" => hotCount.orderCount += 1
          case "pay" => hotCount.payCount += 1
        }
        map.update(cid,hotCount)


    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]]): Unit = {
      other.value.foreach{
        case ( cid, otherHCC ) => {
          val thisHCC: HotCategoryCount = map.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
          thisHCC.clickCount += otherHCC.clickCount
          thisHCC.orderCount += otherHCC.orderCount
          thisHCC.payCount += otherHCC.payCount
          map.update(cid, thisHCC)
        }
      }

    }

    override def value: mutable.Map[String, HotCategoryCount] = {
      map
    }
  }


