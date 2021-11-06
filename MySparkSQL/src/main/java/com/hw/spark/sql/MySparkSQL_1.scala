package com.hw.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.Aggregator

object MySparkSQL_1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("sparkSql_1").getOrCreate()

    import spark.implicits._
    val df = spark.read.json("data/word.json")
    df.createTempView("user1")

 //   spark.udf.register("addName", (x: String) => "name: " + x)

  //  spark.sql("select name ,addName(name) from user")
    val ds = df.as[User1]

    // todo 创建聚合函数
    val udaf = new MyAvg
//    todo 将聚合函数转化为查询的列
   val col: TypedColumn[User1, Double] = udaf.toColumn
  // spark.udf.register("myAvg" ,functions.udaf(udaf))
  ds.select(col).show




    spark.stop()
  }
}
case class User1(name:String,age:Long)
//缓存类型
case class AgeBuffer(var sum:Long,var count:Long)
class MyAvg extends Aggregator[User1 ,AgeBuffer,Double]{
  override def zero: AgeBuffer = {
    AgeBuffer(0L,0L)
  }

  override def reduce(b: AgeBuffer, a: User1): AgeBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: AgeBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }

  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] ={
    Encoders.scalaDouble
  }
}