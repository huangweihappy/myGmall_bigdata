package com.hw.scala.demo

import scala.io.Source

/**
 * 统计单词个数
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // todo 1.从文件中按行读取数据，把数据放到一个集合中
    val source = Source.fromFile("data/word.txt")
    val lines = source.getLines().toArray
  //  println(lines)
    source.close()
    // todo 2.把字符串切分为一个个单词
    val words= lines.flatMap(_.split(" "))
    //println(words)
//    todo 3.把单词进行分组
    val word = words.groupBy(num => num)
    //println(word)
//    todo 4.统计单词的个数
    val ends = word.mapValues(_.size)
//    todo 5.把结果打印到控制台
    println(ends)

  }


}
