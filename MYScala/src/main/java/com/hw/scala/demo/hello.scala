package com.hw.scala.demo

import scala.collection.mutable.ArrayBuffer

object hello {
  def main(args: Array[String]): Unit = {
   val  array = ArrayBuffer(1,2,3,4,5,6,7,8,9)
    val array2 = ArrayBuffer(
      ArrayBuffer(1,2),ArrayBuffer(3,4)
    )
    val array3 = ArrayBuffer(
      "hello scala","hello spark","hello java"
    )
  /*
    println(array.size)// todo array.size获得集合的长度
    val sortInt = array.groupBy(_%2)// todo array.groupBy对数组中的元素进行分组
    println(sortInt)
    println(array.to)// todo array.to把数组转换为Vector

    */
//    println(array2.flatten)// todo array2.flatten扁平化

//   val strArray = array3.flatMap(
//      str =>{
//      str.split(" ")
//    }
//    )// todo array3.flatMap自定义扁平化
////     todo array3.flatMap简化版

//    val str2 = array3.flatMap(_.split(" "))// todo array3.flatMap自定义扁平化
//    println(str2)
//
//    array.insert(2,78)// todo array.insert在指定索引位置插入一个值
//    println(array)


//    array.insertAll(3,ArrayBuffer(21,22,23))// todo  array.insertAll在指定索引处插入多个值
//    println(array)

//       array.clear()// TODO array.clear()清空集合
//       println(array)
//      array.remove(3)// todo   array.remove()删除指定位置上x的元素
//    println(array)
//    array.remove(2,3) // todo  array.remove(x,y)删除指定位置x往后的y个为元素
//    println(array)
//    array.append(12)//todo  array.append(x)在集合末尾追加一个元素x
//    println(array)
//    println(array.companion)//todo  array.companion获取集合的内存地址
//
//    println(array.par) //todo array.par转化为parArray
//    println(array.stringPrefix) //todo array.stringPrefix获取类型
//    println(array.apply(10)) // todo array.apply(x) 获取指定索引x处的元素
//    println(array.fold(5)(_+_)) //todo array.fold(5)(_+_)集合与集合之外的数做聚合
//      array.foreach(println) // todo array.foreach(println) 集合遍历
//    println(array.map(_ * 2)) //todo 对集合中每个元素进行操作
//        val mapNum =array.map(
//          num =>{
//            num *3
//          }
//       )//todo 具体版
//    println(mapNum)

//    println(array.reduce(_ + _))  // todo array.reduce(_ + _) 把集合中的元素聚合
//    array.prepend(45) // todo  array.prepend(x)在集合前面加上一个元素x
//    println(array)
//   todo    array.scan()扫描执行过程
  }
}