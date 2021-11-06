package com.hw.scala.demo

/**
 * * 函数定义：
 * * 1.无参无返回值
 * * 2.有参无返回值
 * * 3.无参有返回值
 * * 4.有参有返回值
 * * 5.有多参无返回值
 * * 6.有多参有返回值
 */
object HelloWorldScala {
	def main(args :Array[String]) : Unit ={
	def test1():Unit={
    println("1无参无返回值")
  }
    test1()
	def test2(name:String):Unit={

	println("2有参无返回值::"+name)

	}
    test2("xiaohong")
	def test3() : String ={

		"3无参有返回值"
	}
		println(test3())

	def test4(name:String)={

		"4有参有返回值"+name

	}

    println(test4("wangwu"))
    def test5(name:String ,age:Int): Unit ={
      println(s"5有多参无返回值==name=${name}==age=${age}=====")
    }
    test5("zhangsan",23)

    def test6(name:String,age:Int): String ={
      s"6有多参有返回值====name=${name}==age=${age}====="
    }
      println(test6("lisi",18))


    def test7(names:String*):Unit ={
      println("7可变参数names----"+names)
    }
    test7()
    test7("name1")
    test7("name1","name2")

    def test8(name:String,password:String = "000000"): Unit ={
      println(s"8参数默认值-----name=${name}-------password=${password}")
    }

    test8("xiaoming")
    test8("zhangheng","123456")

    def test9(password:String="000000",name:String): Unit ={
      println(s"9带名参数------password=${password}---name=${name}---")

    }
    test9(name="songjiang")
	}
 }