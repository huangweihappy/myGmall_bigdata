package com.athw.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req_hotCategory10  extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
  val sc = new SparkContext(conf)
  // todo 统计热点商品排名top10
  //  todo 1.获取原始数据
  val datas: RDD[String] = sc.textFile("data/user_visit_action.txt")
  //  todo 2.对数据进行封装
  //2019-07-17   38   502cdc9-cf95-4b08-8854-f03a25baa917     11  2019-07-17 00:00:29  苹果_-1_-1_null_null_null_null_7

  //  todo 3.获取品类的点击数量
    val chickDatas: RDD[String] = datas.filter(
    str => {
      val words = str.split("_")
      words(6) != "-1"
    }
  )
  val clickCountDatas: RDD[(String, Int)] = chickDatas.map(
    str => {
      val words = str.split("_")
      (words(6), 1)
    }
  ).reduceByKey(_ + _)
  //  todo 4.获取品类的下单数量
  val orderCountDatas: RDD[(String, Int)] = datas.filter(
    str => {
      str.split("_")(8) != "null"
    }
  ).flatMap(
    str => {
      str.split("_")(8)
        .split(",")
        .map((_, 1))
    }
  ).reduceByKey(_ + _)

  //  todo 5.获取品类的支付数量
  val payCountDatas: RDD[(String, Int)] = datas.filter(
    str => {
      str.split("_")(10) != "null"
    }
  ).flatMap(
    words => {
      words.split("_")(10).split(",").map((_, 1))
    }
  ).reduceByKey(_ + _)

  //  todo 6.对数据进行排序（点击，下单，排序）
  val countData: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))]
  = clickCountDatas.cogroup(orderCountDatas,payCountDatas)
  countData.map {
    case (id, (click, order, pay)) => {
      var clickcnt = 0
      var ordercnt = 0
      var paycnt = 0

      val iterator1: Iterator[Int] = click.iterator
      if (iterator1.hasNext) {
        clickcnt = iterator1.next()
      }

      val iterator2: Iterator[Int] = order.iterator
      if (iterator2.hasNext) {
        ordercnt = iterator2.next()
      }

      val iterator3: Iterator[Int] = pay.iterator
      if (iterator3.hasNext) {
        paycnt = iterator3.next()
      }

      (id, (clickcnt, ordercnt, paycnt))

    }
  }.sortBy(_._2,false).take(10).foreach(println)


  //  todo 7. 取top10 打印在控制台


}