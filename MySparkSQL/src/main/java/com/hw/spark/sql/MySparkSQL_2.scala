package com.hw.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.Aggregator

object MySparkSQL_2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("sparkSql_2").getOrCreate()

    import spark.implicits._

    spark.read.format("jdbc").options(
      Map(
      "url" -> "jdbc:mysql://hadoop102:3306/gmall",
      "driver" ->"com.mysql.jdbc.Driver",
      "user"->"root",
      "password"->"123456",
        "dbtable" ->"base_province"
      )
    ).load().show





    spark.stop()
  }
}
