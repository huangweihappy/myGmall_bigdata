package com.athw.spark.req

import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}


object Req_hotCategory10_exer1  extends  App {


  val conf = new SparkConf().setMaster("local[*]").setAppName("Top10")
  val sc = new SparkContext(conf)
  // todo 统计热点商品排名top10
  //  todo 1.获取原始数据
  val dataFile = sc.textFile("data/user_visit_action.txt")
  private val datasRDD: RDD[UserVisitAction] = dataFile.map(
    str => {
      val datas = str.split("_")
      UserVisitAction(
        datas(0), datas(1).toLong, datas(2), datas(3).toLong,
        datas(4), datas(5), datas(6).toLong, datas(7).toLong,
        datas(8), datas(9), datas(10), datas(11), datas(12).toLong

      )
    }
  )
  datasRDD.cache()

  private val clickIds = datasRDD.filter(_.click_category_id != -1).map(
    str => {
        (str.click_category_id.toString, 1)
    }

  ).reduceByKey(_ + _)

  private val orderIds: RDD[(String, Int)] = datasRDD.filter(_.order_category_ids != "null").flatMap(
    str => {
      val words = str.order_category_ids.split(",")
      words.map(
        word => {
          (word, 1)
        }
      )
    }
  ).reduceByKey(_ + _)

  private val payIds: RDD[(String, Int)] = datasRDD.filter(_.pay_category_ids != "null").flatMap(
    str => {
      val words = str.pay_category_ids.split(",")
      words.map(
        word => {
          (word, 1)
        }
      )
    }
  ).reduceByKey(_ + _)

    private val cgroupIds: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickIds.cogroup(orderIds,payIds)
  cgroupIds.map{
    case (id,(click,order,pay)) =>{
      var clickCount = 0
      var orderCount = 0
      var payCount = 0
      val iter1 = click.iterator
      if(iter1.hasNext){
        clickCount += iter1.next()
      }
      val iter2 = order.iterator
      if(iter2.hasNext){
        orderCount += iter2.next()
      }
      val iter3 = pay.iterator
      if(iter3.hasNext){
        payCount += iter3.next()
      }
      (id,(clickCount,orderCount,payCount))
    }
  }.sortBy(_._2,false).take(10).foreach(println)

  sc.stop()
}
