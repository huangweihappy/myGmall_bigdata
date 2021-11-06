package com.athw.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 方法一.map + reduceByKey
 * 方法二:使用countByValue代替map + reduceByKey
 * 方法三:aggregateByKey或者foldByKey
 * 方法四:groupByKey+map
 * 方法五:Scala原生实现wordcount
 * 方法六：combineByKey
 */
object MySparkWordCount5 {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
       val sc = new SparkContext(conf)
    // todo 方法四:groupByKey+map
   val lines  =  sc.textFile("data/word.txt")


    val words = lines.flatMap(_.split(" ")).map(str => (str ,1)).groupByKey().mapValues(_.size)




    words.collect().foreach(println)










  }

}