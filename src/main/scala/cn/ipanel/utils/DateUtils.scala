package cn.ipanel.utils

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar, Locale, TimeZone}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/**
  *
  * 日期工具类
  *
  * @author liujjy
  * @date 2017/12/25 11:03
  */

object DateUtils {

  val YYYYMMDD = "yyyyMMdd"
  val YYYY_MM_DD = "yyyy-MM-dd"
  val YYYY_MM_DD_HHMMSS = "yyyy-MM-dd HH:mm:ss"
  val YYYY_MM_DD_HHMM = "yyyy-MM-dd HH:mm" //lizhy@20190301
  val HH = "HH"
  val MM = "mm"


  /**
    * 检验时间范围，确保日志都在规定的时间内
    *
    * @param timeStr yyyy-MM-dd hh:mm:ss格式字符串
    * @param day     yyyyMMdd格式字符串
    * @return yyyyMMdd格式
    */
  def validateTimeRange(timeStr: String, day: String): Boolean = {
    val startTime = DateTime.parse(timeStr, DateTimeFormat.forPattern(DateUtils.YYYY_MM_DD_HHMMSS))
    val endTime = DateTime.parse(day, DateTimeFormat.forPattern(DateUtils.YYYYMMDD))
    if (startTime.getDayOfYear - endTime.getDayOfYear == 0) true else false
  }

  /**
    * 获取指定日期当天开始时间
    *
    * @param day 日期 yyyyMMdd
    * @return yyyy-MM-dd 00:00:00
    */
  def getStartTimeOfDay(day: String): String = {
    val dateTime = DateTime.parse(day, DateTimeFormat.forPattern(DateUtils.YYYYMMDD))
    dateTime.toLocalDate + " 00:00:00"
  }

  /**
    * 获取指定日期当天最后时间
    *
    * @param day 日期 yyyyMMdd
    * @return yyyy-MM-dd 23:59:59
    */
  def getEndTimeOfDay(day: String): String = {
    val dateTime = DateTime.parse(day, DateTimeFormat.forPattern(DateUtils.YYYYMMDD))
    dateTime.toLocalDate + " 23:59:59"
  }

  /**
    * 时间格式字符串转 DateTime
    * @param dateStr  日期字符串 例如[2018-05-17 11:48:21]
    * @param parttern 定的时间格式字符串 默认yyyy-MM-dd hh:mm:ss
    * @return DateTime对象
    */
  def dateStrToDateTime(dateStr: String, parttern: String = YYYY_MM_DD_HHMMSS): DateTime = {
    val pattern = DateTimeFormat.forPattern(parttern)
    DateTime.parse(dateStr, pattern)
  }

  /**
    * 字符串转unix时间戳
    *
    * @param dateStr  日期字符串
    * @param parttern 指定的时间格式字符串 默认yyyy-MM-dd hh:mm:ss
    * @return
    */
  def dateToUnixtime(dateStr: String, parttern: String = YYYY_MM_DD_HHMMSS): Long = {
    val pattern = DateTimeFormat.forPattern(parttern)
    val dateTime = DateTime.parse(dateStr, pattern)
    dateTime.getMillis / 1000
  }

  def transferYYYY_DD_HH_MMHHSSToDate(date: String) = {
    val pattern = DateTimeFormat.forPattern(YYYY_MM_DD_HHMMSS)
    val dateTime = DateTime.parse(date, pattern)
    dateTime
  }

  /**
    * nginx时间格式化
    *
    * @param timeStr  nginx时间字符串 【例如08/Apr/2018:11:21:13 +0800】
    * @param parttern 指定的时间格式字符串，默认yyyy-MM-dd hh:mm:ss
    * @return 返回指定时间格式字符串
    */
  def nginxTimeFormate(timeStr: String, parttern: String = YYYY_MM_DD_HHMMSS): String = {
    var result = ""
    try {
      if (timeStr.nonEmpty && timeStr.contains("'T'")) {
        result = utcDateFormat(timeStr).toString(parttern)
      } else {
        val formatter = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
        val date = formatter.parse(timeStr)
        val sdf = new SimpleDateFormat(parttern)
        result = sdf.format(date)
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    result
  }


  /**
    * 时间戳格式化
    *
    * @param timestamp    windows时间戳
    * @param formatString 格式化字符串
    * @return
    */
  def timestampFormat(timestamp: Long, formatString: String): String = {
    new DateTime(timestamp).toString(formatString)
  }

  /**
    * 计算N天后的日期
    **/
  def getNDaysAfter(n: Int, startDay: String, formatString: String = YYYYMMDD): String = {
    val pattern = DateTimeFormat.forPattern(formatString)
    val dateTime = DateTime.parse(startDay, pattern)
    dateTime.plusDays(n).toString(formatString)
  }

  /**
    * 获取当天日期
    *
    * @return yyyyMMdd格式
    */
  def getNowDate(format: String = YYYYMMDD): String = {
    val dateTime: DateTime = new DateTime()
    dateTime.toString(format)
  }
  /** *
    * 计算N天前的日期
    *
    * @param n
    * @param startDay
    * @param formatString
    * @return
    */
  def getNDaysBefore(n: Int, startDay: String, formatString: String = YYYYMMDD): String = {
    val time = DateUtils.dateToUnixtime(startDay, formatString)
    val timeStamp = (time - (n * 24 * 3600)) * 1000
    val simpleDateFormat = new SimpleDateFormat(formatString)
    val date = new Date(timeStamp)
    simpleDateFormat.format(date)
  }


  /**
    * 获取昨日日期
    *
    * @param formatString 格式日期
    * @return yyyyMMdd格式
    */
  def getYesterday(formatString: String = YYYYMMDD): String = {
    val dateTime: DateTime = new DateTime()
    dateTime.plusDays(-1).toString(formatString)
  }

  /**
    * unix时间戳格式化
    *
    * @param unixTime unix时间戳
    * @return 返回指定格式日期 默认yyyy-MM-dd
    */
  def unixTimeToDate(unixTime: Long, format: String = YYYY_MM_DD): String = {
    val dateTime = new DateTime(unixTime * 1000)
    dateTime.toString(format)
  }
  /**
    * 毫秒时间戳格式化
    * added by lizhy@20190301
    * @param timeStamp unix时间戳
    * @return 返回指定格式日期 默认yyyy-MM-dd
    */
  def millisTimeStampToDate(timeStamp: Long, format: String = YYYY_MM_DD) = {
    val dateTime = new DateTime(timeStamp)
    dateTime.toString(format)
  }

  /**
    * yyyyMMdd与yyyy_MM_dd转换
    *
    * @param date    日期yyyyMMdd
    * @param format1 时间格式1
    * @param format2 时间格式2
    * @return 返回指定格式字符串 
    */
  def transformDateStr(date: String, format1: String = YYYYMMDD, format2: String = YYYY_MM_DD) = {
    val pattern = DateTimeFormat.forPattern(format1)
    val dateTime = DateTime.parse(date, pattern)
    dateTime.toString(format2)
  }

  def getTimestamp(t: String, format1: String = "yyyy-MM-dd HH:mm:ss:SSS"): Long = {
    val dateFormat = new SimpleDateFormat(format1)
    dateFormat.parse(t).getTime ///1000
  }


  /**
    * 获取指定日期的后一天
    *
    * @param date   日期默认yyyyMMdd 格式
    * @param format 时间格式 默认和date格式相同
    * @param targetFormat 需要返回时间格式,默认yyyyMMdd
    * @return 返回指定格式时间yyyyMMdd
    */
  def getAfterDay(date: String, format: String = YYYYMMDD,targetFormat: String=YYYYMMDD) = {
    val pattern = DateTimeFormat.forPattern(format)
    val dateTime = DateTime.parse(date, pattern)
    dateTime.plusDays(1).toString(targetFormat)
  }

  /**
    * 从日期字符串中获取小时值
    *
    * @param time    yyyy-MM-dd HH:mm:ss
    * @param format1 时间格式yyyy-MM-dd HH:mm:ss
    * @param format2 时间格式HH
    * @return 返回指定格式HH
    */
  def getHourFromDateTime(time: String, format1: String = YYYY_MM_DD_HHMMSS, format2: String = HH) = {
    val pattern = DateTimeFormat.forPattern(format1)
    val dateTime = DateTime.parse(time, pattern)
    dateTime.toString(format2)
  }

  /**
    * 从日期中获取分钟时刻值
    *
    * @param time
    * @param format1
    * @param format2
    * @return
    */
  def getMinuteFromDateTime(time: String, format1: String = YYYY_MM_DD_HHMMSS, format2: String = MM) = {
    val pattern = DateTimeFormat.forPattern(format1)
    val dateTime = DateTime.parse(time, pattern)
    dateTime.toString(format2)
  }

  /**
    * nginx时间格式化
    *
    * @param timeStr  nginx时间字符串 【例如08/Apr/2018:11:21:13 +0800】
    * @param parttern 指定的时间格式字符串，默认yyyy-MM-dd hh:mm:ss
    * @return DateTime 对象
    */
  def nginxTimeToDateTime(timeStr: String, parttern: String = YYYY_MM_DD_HHMMSS): DateTime = {
    var dateTime: DateTime = null
    try {
      if (timeStr.nonEmpty && timeStr.contains("'T'")) {
        dateTime = utcDateFormat(timeStr)
      } else {
        val formatter = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
        val date = formatter.parse(timeStr)
        dateTime = new DateTime(date)
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    dateTime
  }

  /**
    * utc时间格式化
    *
    * @param dateStr 类似 "2018-05-07T10:35:13.000Z" utc时间字符串
    * @return
    */
  def utcDateFormat(dateStr: String): DateTime = {
    val utc = TimeZone.getTimeZone("UTC")
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    format.setTimeZone(utc)
    new DateTime(format.parse(dateStr))
  }
  /** 获取本周第一天,本月第一天,本季度第一天,本年度第一天,指定天数前的日期 added by lizhy@20181030 */
  //本周第一天
  def getFirstDateOfWeek(date: String): String = {
    val weekFirstDayNum = DateUtils.dateStrToDateTime(date, DateUtils.YYYYMMDD).dayOfWeek().getAsString.toInt
    //    val plusDays = if(weekFirstDayNum == 7) 0 else -weekFirstDayNum
    val plusDays = -weekFirstDayNum + 1
    DateUtils.dateStrToDateTime(date, DateUtils.YYYYMMDD).plusDays(plusDays).toString(DateUtils.YYYYMMDD)
  }

  //本月第一天
  def getFirstDateOfMonth(date: String): String = {
    val dt = DateUtils.dateStrToDateTime(date, DateUtils.YYYYMMDD)
    dt.plusDays(1 - dt.getDayOfMonth).toString(DateUtils.YYYYMMDD)
  }
  //本季度第一天
  def getFirstDateOfQuarter(date: String): String = {
    val dt = DateUtils.dateStrToDateTime(date, DateUtils.YYYYMMDD)
    val year = dt.getYear
    val month = dt.getMonthOfYear - (dt.getMonthOfYear - 1) % 3
    new DateTime(year, month, 1, 0, 0).toString(DateUtils.YYYYMMDD)
  }
  //本年度第一天
  def getFirstDateOfYear(date: String): String = {
    val dt = DateUtils.dateStrToDateTime(date, DateUtils.YYYYMMDD)
    val year = dt.getYear
    new DateTime(year, 1, 1, 0, 0).toString(DateUtils.YYYYMMDD)
  }
  //指定天数前的日期
  def getDateByDays(date: String, days: Int): String = {
    val dt = DateUtils.dateStrToDateTime(date, DateUtils.YYYYMMDD)
    dt.plusDays(-days + 1).toString(DateUtils.YYYYMMDD)
  }
  //得到本月最后一天
  def getMonthEnd(date: String): String = {
    import java.text.SimpleDateFormat
    import java.util.Calendar
    val df = new SimpleDateFormat("yyyyMMdd")
    val date1 = df.parse(date)
    val c = new GregorianCalendar()
    c.setTime(date1)
    c.set(Calendar.DATE, 1)
    c.roll(Calendar.DATE, -1)
    df.format(c.getTime())
  }

  /**
    * 获取当前周星期一
    */
  def getCalendar(date: String): String = {
    import java.text.SimpleDateFormat
    import java.util.Calendar
    val df = new SimpleDateFormat("yyyyMMdd")
    val date1 = df.parse(date)
    //   val c = Calendar.getInstance()

    val c = new GregorianCalendar()
    c.setTime(date1)
    c.setFirstDayOfWeek(Calendar.MONDAY);
    c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
    // c.add(Calendar.DAY_OF_WEEK, (-1) * c.get(Calendar.DAY_OF_WEEK) + 2)
    df.format(c.getTime())

  }

  import java.text.SimpleDateFormat
  import java.util.Calendar

  /**
    *
    * Description: 获取标准格式的时间
    *
    * @lastUpdate 2019年1月31日 下午5:17:25
    * @param calendar
    * @return String
    */
  def getDateString(calendar: Calendar): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(calendar.getTime)
  }

  def get_end_time(start: String, duration: String): String = {
    var end_time = ""
    try {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val start_time = dateFormat.parse(start).getTime
      val hms = for (i <- duration.split(":")) yield i.toInt
      val end: Long = (hms(0) * 3600 + hms(1) * 60 + hms(2)) * 1000 + start_time
      end_time = dateFormat.format(new Date(end))
    } catch {
      case t: Throwable => end_time = start
    }
    end_time
  }

  /**
    *
    * Description: 获取两个时间之间相差的秒数
    *
    * @lastUpdate 2019年2月1日 上午9:15:46
    * @param min
    * @param max
    * @return long
    */
  def getDiffSeconds(min: Date, max: Date): Long = (max.getTime - min.getTime) / 1000

  def date_Format(time: String): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: String = sdf.format(new Date(time.toLong * 1000L))
    date
  }

  def main(args: Array[String]): Unit = {
    /*val nginx_date = "27/Feb/2008:10:12:44 +0800"
    val date = nginxTimeFormate(nginx_date)
    println(date)*/
    val str = "2018-05-03 08:38:03"
    val str2 = "2018-0507T10:35:13.000Z"
    val str3 = "081/Apr/2018:11:21:13 +0800"

    val str4 = "08/Apr/2018:11:21:13 +0800"
    val date = nginxTimeToDateTime(str4)
    println(date)

    val str1 = "2018-05-17 11:22:18"
    println(dateStrToDateTime(str1, YYYY_MM_DD_HHMMSS))

    val dateTime = new DateTime()

    dateTime.getHourOfDay

  }
}
