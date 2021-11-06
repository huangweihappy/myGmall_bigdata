package com.athw.spark.req

object TestList {
  def main(args: Array[String]): Unit = {
    //  准备过滤所需要的数据（因为我们只想统计主要页面跳转的转化率，不考虑其他页面的跳转）
    val flagIds  = List(1,2,3,4,5,6,7)
    //  zip 拉链操作，得到的数据格式为  (1,2)(2,3)(3,4)(4,5)(5.6)(6.7)
    val flagToIds = flagIds.zip(flagIds.tail)

    println("tail-->"+flagIds.tail)
    println("init-->"+flagIds.init)
    println("head-->"+flagIds.head)
    println("last-->"+flagIds.last)


    println(flagToIds)
  }

}
