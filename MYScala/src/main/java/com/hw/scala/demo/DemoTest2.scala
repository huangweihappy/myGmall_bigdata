package com.hw.scala.demo

object DemoTest2 {
  def main(args: Array[String]): Unit = {
    val list = List(
      ("zhangsan", "河北", "鞋"), ("lisi", "河北", "衣服"), ("wangwu", "河北", "鞋"), ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"), ("wangwu", "河南", "鞋"), ("zhangsan", "河南", "鞋"), ("lisi", "河北", "衣服"),  ("wangwu", "河北", "鞋"),
      ("zhangsan", "河北", "鞋"), ("lisi", "河北", "衣服"), ("wangwu", "河北", "帽子"), ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"), ("wangwu", "河南", "帽子"), ("zhangsan", "河南", "鞋"), ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),("lisi", "河北", "衣服"), ("wangwu", "河北", "电脑"), ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),("wangwu", "河南", "电脑"), ("zhangsan", "河南", "电脑"), ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子")
    )
     val list1 = list.map(
        str =>{
          ((str._2,str._3),1)
        }
      ).groupBy(_._1).map(
        str =>{
          (str._1,str._2.size)
        }
      ).toList.map(
        str =>{
          (str._1._1,(str._1._2,str._2))
        }
      ).groupBy(_._1).mapValues(
        list =>{
          list.map(
            str =>{
              str._2
            }
          )
        }
      ).mapValues(_.sortBy(_._2)(Ordering.Int.reverse))
    println(list1)






  }
}
