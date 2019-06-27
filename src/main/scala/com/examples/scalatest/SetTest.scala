package com.examples.scalatest

object SetTest {

  def main(args: Array[String]): Unit = {
      "1 ,2 ,3".trim.split(",").foreach(x=> println(x.trim.length))





  }

  def folde(): Unit ={

    val set = Set("1-true","2-false","3-true")
    set.filter(_.contains("true")).foreach(println(_))

    val fooList = Foo("Hugh Jass", 25, 'male) :: Foo("Biggus Dickus", 43, 'male) :: Foo("Incontinentia Buttocks", 37, 'female) :: Nil

    val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
      val title = f.sex match {
        case 'male => "Mr."
        case 'female => "Ms."
      }
      z :+ s"$title ${f.name}, ${f.age}"
    }
  }
}
