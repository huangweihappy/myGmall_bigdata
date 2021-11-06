package com.hw.scala.demo

object Test_function_1 {
  def main(args: Array[String]): Unit = {
    // todo 基本语法
    def test(a :Int,b: Double,c :String) : String ={
      (a + b).toString +c
    }


    println(test(1, 2.5, "hello") )

  }


}
