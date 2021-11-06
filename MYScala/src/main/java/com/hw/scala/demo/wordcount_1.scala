package com.hw.scala.demo

import scala.io.Source

/**
 * 统计单词个数
 */
object wordcount_1 {
  def main(args: Array[String]): Unit = {
    // todo 1.从文件中按行读取数据，把数据放到一个集合中
    val source = Source.fromFile("data/word.txt")
    val words = source.getLines()//获取到文件中的每一行
      .toArray//把每一行String都放到数据的元素中
      .flatMap(_.split(" ")) //把数组中的元素都进行切分为一个个单词
      .groupBy(num => num)//按单词进行分组
      .mapValues(_.size)//统计单词个数

    println(words)

    source.close()
  }


}
