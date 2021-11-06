package com.hw.spark.sql

import org.apache.spark.sql.SparkSession

object MyFirstSparkSql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("sparkSql").getOrCreate()

    import  spark.implicits._
    val df = spark.read.json("data/word.json")
    df.createTempView("user")
  //  df.select("name").show

   // spark.sql("select name ,age from user").show

    val rdd1 = spark.sparkContext.makeRDD(List((21,"sunshangxiang"),(23,"sunce")))
   // val frame = rdd1.toDF("age","name")
  //  val ds = frame.as[User]
 //   ds.show
    rdd1.map{
      case (age,name) =>User(age,name)
    }.toDS().show

    spark.stop()
  }

}
case  class User(age:Long,name:String)