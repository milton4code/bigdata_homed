package cn.ipanel.utils

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import cn.ipanel.utils.DateUtils.YYYY_MM_DD_HHMMSS
import org.apache.commons.lang.StringUtils
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable

/**
  * 日志工具
  */
object LogUtils {

  /**
    * 时间区间拆分为半小时一片
    *
    * @param day        计算哪天的 yyyy-MM-dd
    * @param start_time 开始时间 yyyy-MM-dd HH:mm:ss
    * @param end_time   结束时间 yyyy-MM-dd HH:mm:ss
    * @return Set[(hour,Minute)]
    **/
  def divideTime(day: String, start_time: String, end_time: String, format: String = YYYY_MM_DD_HHMMSS): mutable.Set[(Int, Int)] = {
    val pattern = DateTimeFormat.forPattern(format)
    val sday = DateTime.parse(s"$day 00:00:00", pattern)
    val eday = DateTime.parse(s"$day 23:59:59", pattern)
    val real_start = DateTime.parse(start_time, pattern)
    val real_end = DateTime.parse(end_time, pattern)
    //计算开始时间往迟的算
    var start = if (real_start.getMillis > sday.getMillis) real_start else sday
    //计算结束时间往早的算
    val end = if (real_end.getMillis > eday.getMillis) eday else real_end
    var range_secend = (end.getMillis - start.getMillis) / 1000

    import scala.collection.mutable.Set
    var mutableSet: mutable.Set[(Int, Int)] = Set()
    while (range_secend >= 0) {
      var h = start.getHourOfDay
      var m = if (start.getMinuteOfHour >= 30) 60 else 30
      mutableSet.add((h, m)) //时间片增加

      start = start.plusMinutes(30) //时间向前30分钟
      range_secend = range_secend - 1800 //时间区间减半小时
      if (range_secend < 0) { //最后一片时间片
        var h = end.getHourOfDay
        var m = if (end.getMinuteOfHour >= 30) 60 else 30
        mutableSet.add((h, m)) //时间片增加
      }
    }
    mutableSet
  }


  /**
    * 记录程序运行时间
    * @param name 程序名称 或者功能
    */
  class Stopwatch(name:String) {
    private val start = System.currentTimeMillis()
    override def toString() = {
      val dateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
      val fileFix = DateUtils.getNowDate()
      val msg =  s"$dateTime , $name  耗时 " +  (System.currentTimeMillis() - start) + " ms"
      val  bw = new BufferedWriter(new FileWriter(s"/r2/bigdata/timer_$fileFix.log", true))
      bw.write(msg + "\n")
      bw.flush()
      bw.close()
      msg
    }
  }


  /**
    * 将字符串转换为map对象
    *
    * @param str        字符串
    * @param delimiter1 k-v对之间的分隔符,默认,
    * @param delimiter2 k-v键值对分隔符 ,默认=
    * @return mpa  str_to_map('aaa:11&bbb:22', '&', ':') =>  {"aaa":"11","bbb":"22"}
    */
  def str_to_map(str: String, delimiter1: String = ",", delimiter2: String = ":"): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    if (!StringUtils.isEmpty(str)) {
       str.split(delimiter1).foreach(word => {
        val splits = word.split(delimiter2)
        if (splits.length == 2) {
          map.put(splits(0), splits(1))
        } else if( splits.length > 2) {
          val key = word.substring(0,word.indexOf(delimiter2)).trim
          val value = word.substring(word.indexOf(delimiter2)).trim
          map.put(key,value)
        }else{
          map.put(splits(0), null)
        }
      })
    }
    map

  }

  def str_to_map_new(str: String, delimiter1: String = ",", delimiter2: String = ":",source:String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    if (!StringUtils.isEmpty(str)) {
      str.split(delimiter1).foreach(word => {
        val splits = word.split(delimiter2)
        if (splits.length == 2) {
          map.put(splits(0), splits(1))
        } else if( splits.length > 2) {
          val key = word.substring(0,word.indexOf(delimiter2)).trim
          val value = word.substring(word.indexOf(delimiter2)).trim
          map.put(key,value)
        }else{
          map.put(splits(0), null)
        }
      })
    }
    map.put("logsouce",  source)
    map

  }


  /**
    * 当前操作系统是否为windows
    * @return
    */
  def isWindows():Boolean ={
    System.getProperties().get("os.name").toString.toLowerCase.contains("windows")
  }

}
