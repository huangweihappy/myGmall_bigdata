package com.athw.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
1	user_id	Long	用户的ID
2	session_id	String	Session的ID
3	page_id	Long	某个页面的ID
4	action_time	String	动作的时间点
 */
object Req_zhuanHua  extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
  val sc = new SparkContext(conf)
// todo 读取文件获取数据
  val lines: RDD[String] = sc.textFile("data/user_visit_action.txt")
//  对数据进行封装
  val dataRDD  = lines.map(
    str =>{
      val datas  = str.split("_")
      UserVisitAction(
        datas(0), datas(1).toLong, datas(2), datas(3).toLong,
        datas(4), datas(5), datas(6).toLong, datas(7).toLong,
        datas(8), datas(9), datas(10), datas(11), datas(12).toLong,

      )
    }
  )
//  todo 把数据放到内存中重复使用，减少磁盘读取IO，提高效率
  dataRDD.cache()
//  准备过滤所需要的数据（因为我们只想统计主要页面跳转的转化率，不考虑其他页面的跳转）
  val flagIds  = List(1,2,3,4,5,6,7)
//  zip 拉链操作，得到的数据格式为  (1,2)(2,3)(3,4)(4,5)(5.6)(6.7)
  val flagToIds = flagIds.zip(flagIds.tail)

//  todo 对分母进行运算
//  按照要求对数据进行过滤留下所有PageId在flagIds中的数据
  private val filterRDD: RDD[UserVisitAction] = dataRDD.filter(
    str => {
      //留下flagIds中包含的pageId
      flagIds.init.contains(str.page_id.toInt)
    }
  )

//  对留下来的数据进行转化并统计点击量并放到map中（ k 表示pageId ）（v 表示被点击的个数）
  private val pageId: Map[Long, Int] = filterRDD.map(
    str => {
      (str.page_id, 1)
    }
  ).reduceByKey(_ + _).collect().toMap

// todo 计算分子
//  根据session_id进行分组（表示这些页面是同一个用户访问的）
  private val groupRdd: RDD[(String, Iterable[UserVisitAction])] = dataRDD.groupBy(_.session_id)
//  对分组内的数据按照点击时间进行排序就可以知道用户从什么页面跳转到什么页面
  private val sessionRDD: RDD[(String, List[(Int, Int)])] = groupRdd.mapValues(
    iter => {
      val strids = iter.toList.sortBy(_.action_time).map(
        str => {
          (str.page_id.toInt)
        }
      )
//      拉链操作的到数据为（2，3）...的格式
      strids.zip(strids.tail)
    }
  )

// todo 扁平化 只保留后面的数据
  private val pagetoIds: RDD[(Int, Int)] = sessionRDD.flatMap(_._2)
//  过滤点不是主要页面跳转的数据，然后统计出（page1,toPage）的个数
  private val reduceRDD  = pagetoIds.filter(
    str =>{
      flagToIds.contains(str)
    }
  ).map(
    str => {
      (str, 1)
    }
  ).reduceByKey(_ + _)

// todo 求得结果打印到控制台

  reduceRDD.foreach{
   case ((page1,page2),num) =>{
      println(s"${page1}跳转到${page2}的转化率为"+(num.toDouble/pageId.getOrElse(page1,1)))
    }
  }






}