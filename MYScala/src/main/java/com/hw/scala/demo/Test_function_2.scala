package com.hw.scala.demo

object Test_function_2 {
  def main(args: Array[String]): Unit = {

    def fun1(): String = {
      "zhangsan"
    }
    val a = fun1
    val b = fun1 _
    val c : ()=>Unit = fun1
    println(a)
    println(b())


  }


}
