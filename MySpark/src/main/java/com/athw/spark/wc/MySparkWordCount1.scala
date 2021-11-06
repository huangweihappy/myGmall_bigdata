package com.athw.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1 groupBy  =
 * 2 groupByKey  =
 * 3 ReduceByKey=
 * 4 aggregateByKey=
 * 5 foldByKey=
 * 6 combineByKey=
 * 7 countByKey  =
 * 8 countByValue=
 * 9 reduce
 * 10 fold
 * 11 aggreagre
 */

object MySparkWordCount1{

  def main(args: Array[String]): Unit = {


  val conf =   new SparkConf().setMaster("local").setAppName("wordCount")
   val sc =  new SparkContext(conf)

       val  lines = sc.textFile("data/word.txt")
      //"hello java hello scala ..."
    val words = lines.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)
    // map(hello ->list((hello,1),(hello,1)...),java ->list((java,1),(java,1)...))
    val wordCount = words.mapValues(
      list =>{
        list.reduce(
          (t1,t2) =>{
            (t1._1,t1._2+t2._2 )
          }
        )._2
      }
    )

    wordCount.collect().foreach(println)

  }


}
