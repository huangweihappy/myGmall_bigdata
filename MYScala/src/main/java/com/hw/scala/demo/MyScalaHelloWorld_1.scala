package com.hw.scala.demo
/*
        *
       ***
      *****
     *******
    *********
   ***********
  *************
 ***************
*****************
1 2 3 4 5  6  7   8   9
1 3 5 7 9  11 13  15  17
8 7 6 5 4  3  2   1   0
 */
object MyScalaHelloWorld_1 {
  def main(args: Array[String]): Unit = {
    val len = 9
    for (i <- 1 to len) {
      print(" " * (len - i))
      println("*" * (2 * i - 1))

    }
  }
}