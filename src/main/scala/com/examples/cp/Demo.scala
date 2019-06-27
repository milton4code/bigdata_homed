package com.examples.cp

import com.google.common.hash.Hashing

import scala.collection.mutable
import scala.io.Source

/**
  * c33 18
  *
  * 历史共计2000多期数据 6位数
  * 统计 10亿条中哪18码 是出现次数最多的 平均多少期出一次 现在几期没出来了 是不是接近了 历史最大值
  * 这个是第一步 就统计前几十位
  *
  * http://www.17500.cn/getData/ssq.TXT
  */
object Demo {


  def main(args: Array[String]): Unit = {
      println("a,b,".split(",").length)


  }

  /**
    * 生成33选18 数据库
    */
  def getRandomCode(target:Int=7,seed:Int=18)={
    val targetMap = new mutable.HashMap[String,String]()
    (1 to 6).combinations(seed).toList.foreach(x=>{
      val code = x.sortWith(_.compareTo(_) < 0)
      println(code.mkString(","))
//      val md5_code =Hashing.md5().hashBytes( code.slice(0,target).mkString(",").getBytes()).toString
//      targetMap.put(md5_code,code.mkString(","))
    })
   targetMap
  }

  /**
    * 获取历史中奖号码
    * @return
    */
  def getSeeds(target:Int=8) = {
    val url="http://www.17500.cn/getData/ssq.TXT"
    //链接url，获取返回值
    val fileContent = Source.fromURL(url,"utf-8")
    val seedMap = new mutable.HashMap[String,String]()
    fileContent.getLines().foreach(x=>{
      //中奖号码
      val seed =  x.split(" ").slice(2,target).sortWith(_.compareTo(_) < 0).mkString(",")
      //MD5 +
      val seedMd5 = Hashing.md5().hashBytes(seed.getBytes()).toString
      seedMap.put(seedMd5,seed)
    })
    seedMap
  }
}
