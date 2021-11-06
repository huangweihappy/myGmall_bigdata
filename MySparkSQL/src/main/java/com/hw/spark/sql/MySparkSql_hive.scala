package com.hw.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

object MySparkSql_hive {
  {
    System.setProperty("HADOOP_USER_NAME","atguigu")
  }
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport().master("local[*]").appName("sparkSql").getOrCreate()
    import spark.implicits._
// 用户行为表  user_visit_action  产品表   product_info  城市表 city_info

    spark.udf.register("city_mark",functions.udaf(new myAggc))


    spark.sql(
      """
        |select
        |    t2.area,
        |    t2.product_name,
        |    t2.click_count,
        |    mark
        |
        |from
        |    (select
        |         t1.area,
        |         t1.product_name,
        |         t1.click_count,
        |         mark,
        |         rank() over (partition by t1.area order by t1.click_count desc ) rk
        |     from
        |
        |         (select
        |              ci.area,pi.product_name ,
        |              count(1) click_count,
        |              city_mark(city_name)  mark
        |          from
        |              mydb.user_visit_action usa
        |                  join mydb.city_info ci
        |                       on ci.city_id = usa.city_id
        |                  join mydb.product_info pi
        |                       on pi.product_id = usa.click_product_id
        |          where click_product_id != -1
        |          group by area,product_name) t1) t2
        |where rk <=3;
        |""".stripMargin).show
    //createTempView("marks")

//    spark.sql(
//      """
//        |create table mydb.result as select * from marks
//        |""".stripMargin).show







    spark.close()

  }
}
case class  CityCount(var totalCount:Long,var cityMap : mutable.Map[String,Long])
class myAggc extends Aggregator[String,CityCount,String]{
  override def zero: CityCount = CityCount(0L,mutable.Map.empty[String,Long])

  override def reduce(b: CityCount, a: String): CityCount = {
    b.totalCount += 1
   b.cityMap(a)  =  b.cityMap.getOrElse(a,0L) + 1
    b
  }

  override def merge(b1: CityCount, b2: CityCount): CityCount = {
    b1.totalCount = b1.totalCount + b2.totalCount
    b2.cityMap.foreach {
      case (city, cnt) => {
        b1.cityMap(city) = b1.cityMap.getOrElse(city, 0L) + cnt
      }
    }
    b1
  }

  override def finish(re: CityCount): String = {
    val count = re.totalCount
    val list = re.cityMap.toList.sortBy(_._2)(Ordering.Long.reverse)
    if (list.size <= 2) {
      list.map{
        case (city, cnt) => {
          city+" "+(cnt*1000/count).toDouble / 10 +"%"

      }
    }.mkString(",")
    }else {
      val take2: List[(String, Long)] = list.take(2)
      val tmp: List[(String, Long)] = take2 :+ ("其他",count-take2.map(_._2).sum)
        tmp.map{
          case (city, cnt) => {
            city+" "+(cnt*1000/count).toDouble / 10 +"%"

          }
        }.mkString(",")
    }

  }

  override def bufferEncoder: Encoder[CityCount] =Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
