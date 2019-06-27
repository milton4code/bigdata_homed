package cn.ipanel.homed.general

import cn.ipanel.common.{Constant, DBProperties, SparkSession}
import cn.ipanel.customization.wuhu.users.UsersProcess
import cn.ipanel.homed.general.CountUsers.delMysql
import cn.ipanel.utils.DateUtils.YYYY_MM_DD_HHMMSS
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils, RegionUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
/**
  * 在线用户统计（依赖CountUsers用户总量统计结果表、MysqlToHive数据转移）
  * 20180206 create by wenbin
  * updated by lizhy@20181220
  * */
object OnlineUser {
  val sqlHB = DBUtils
  val dt=DateUtils
  val stablea = "t_da" //取用户总量
  val stableh = "t_home" //取家庭(设备)区域地址
  val stableodev="t_online_dev_by_day" //存表：在线设备数
  val stablehalf="t_online_dev_by_halfhour" //存表：在线用户统计(设备)
  val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码 added @20190421
  def main(args: Array[String]): Unit = {
   val sparkSession = SparkSession("OnlineUser")
    val sc=sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    if(args.length != 1){
      println("参数不正确,请输入日期 [20180528]")
      sys.exit(-1)
    }

    val day = args(0)
    val nextDay = DateUtils.getAfterDay(day)

    delMysql(day,stableodev)
    delMysql(day,stablehalf)

    onlineDay(day,nextDay,sc,sqlContext)

    OnlineUsersAndTime.process(day,sqlContext)

    sc.stop()
  }

  /**
    * 按天统计各区域在线设备、人数
    * */
  def onlineDay(day:String,nextDay:String,sc:SparkContext,sqlContext:SQLContext): Unit ={
    import sqlContext.implicits._
    //先算分母，各区总设备、总用户
    val sdevs="(SELECT a.device_id as f_device_id,device_type as f_device_type,c.region_id as f_region_id,c.city_id as f_city_id,c.province_id as f_province_id from "+
      "device_info a JOIN address_device b ON a.device_id=b.device_id and a.status=1 JOIN address_info c on c.address_id=b.address_id "+
      s"WHERE a.f_create_time<$nextDay  ) as t_devs"
    //f_device_id,f_device_type,f_region_id,f_city_id,f_province_id
    val devs=sqlHB.loadMysql(sqlContext,sdevs, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    val dev_num=devs.groupBy("f_device_type","f_region_id","f_city_id").count()
      .withColumnRenamed("count","f_total_dev").persist()

    val day_ =day.replace("-","")
    //设备一定属于家庭，就用家庭的地址区域(没设备总数，所以没按各个源累加)
    val shome=s"(select distinct f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name from $stableh where f_date='$day_') as t_home_addr "
    val dev_addr= sqlHB.loadMysql(sqlContext, shome, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)

    val region_token=RegionUtils.getRootRegion

    //下面算分子，homeid关联方式不同于分母
    val saddress = s"(SELECT DISTINCT(home_id) as f_home_id,region_id as f_region_id,city_id as f_city_id from address_info where status!=9 and (city_id=$region_token or province_id=$region_token)) as t_address"
    val addrDF=sqlHB.loadMysql(sqlContext,saddress, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      .persist()
    val loginDF =
    //各地定制化开发
    clusterRegion match {
      case "340200" => { //芜湖
        UsersProcess.loginDevDf(day,sqlContext,loadFromHive(day,nextDay,sqlContext)).join(addrDF,Seq("f_home_id"))
      }
      case _ => //默认
        loadFromHive(day,nextDay,sqlContext).join(addrDF,Seq("f_home_id"))
    }
    loginDF.registerTempTable("t_loginDF")
    sqlContext.cacheTable("t_loginDF")

    //设备维度统计
    val devDF2=sqlContext.sql("SELECT f_region_id,f_city_id,f_device_type,COUNT(DISTINCT f_user_id) as f_dev_num,'' as f_device_ids " +
      "from t_loginDF " +
      "group by f_region_id,f_city_id,f_device_type")
      .join(dev_addr,Seq("f_region_id","f_city_id")) //加省地址
      .join(dev_num,Seq("f_region_id","f_city_id","f_device_type"))
      .withColumn("f_date",lit(day_))

    //半小时分片的设备维度统计
    val halfHourDev=sqlContext.sql(
      """
        |SELECT f_region_id,f_city_id,cast(f_device_type as STRING) as f_device_type,f_device_id,
        |cast(f_login_time as STRING) as f_login_time,cast(f_logout_time as STRING) as f_logout_time
        |from t_loginDF
      """.stripMargin)
      .map(x=> //半小时分片统计在线设备，flatmap方便裂变
        (x.getAs[Long]("f_city_id"),
          x.getAs[Long]("f_region_id"),
          x.getAs[String]("f_device_type"),
          x.getAs[String]("f_device_id"),
          x.getAs[String]("f_login_time"),
          x.getAs[String]("f_logout_time")) )
      .persist()
    println("action先算出halfHourDev.count，并清理内存",halfHourDev.count())
    sqlContext.clearCache()

    val day2=dt.transformDateStr(day)
    halfHourDev.flatMap(v=>
      divideTime(day2,v._5,v._6).map(vv=>(v._1,v._2,v._3,v._4,vv._1,vv._2 )))
      .toDF("f_city_id","f_region_id","f_device_type","f_device_id","f_hour","f_timerange")
      .registerTempTable("t_half_hour_dev")

    val res_half_hour_dev=sqlContext.sql("SELECT f_region_id,f_city_id,f_device_type,f_hour,f_timerange,COUNT(DISTINCT f_device_id) as f_dev_num, '' as f_device_ids " +
      "from t_half_hour_dev " +
      "group by f_region_id,f_city_id,f_device_type,f_hour,f_timerange")
      .join(dev_addr,Seq("f_region_id","f_city_id")) //加省地址
      .join(dev_num,Seq("f_region_id","f_city_id","f_device_type"))
      .withColumn("f_date",lit(day_))

      val hisCntDf = hisOnlineUserCount(day_,sc,sqlContext)
      val cntBydayDf = hisCntDf.join(devDF2,devDF2("f_date")===hisCntDf("h_date") && devDF2("f_device_type")===hisCntDf("h_device_type") && devDF2("f_region_id")===hisCntDf("h_region_id"),"outer")
        .selectExpr("nvl(f_date,h_date) as f_date","f_device_ids","nvl(f_device_type,h_device_type) as f_device_type","nvl(f_dev_num,0) as f_dev_num","nvl(f_total_dev,0) as f_total_dev",
          "nvl(f_province_id,h_province_id) as f_province_id","nvl(f_province_name,h_province_name) as f_province_name", "nvl(f_city_id,h_city_id) as f_city_id",
          "nvl(f_city_name,h_city_name) as f_city_name","nvl(f_region_id,h_region_id) as f_region_id","nvl(f_region_name,h_region_name) as f_region_name","f_hist_user_count")
      //保存按设备维度统计开机用户
      sqlHB.saveToMysql(cntBydayDf, stableodev, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    //保存按半小时维度统计开机用户
    sqlHB.saveToMysql(res_half_hour_dev, stablehalf, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)

  }

  /**
    * 读取hive数据
    * */
  def loadFromHive(day:String, nextDay:String, sqlContext:SQLContext): DataFrame ={
    sqlContext.sql("use bigdata")
    sqlContext.sql(
      s"""
         |select cast(f_user_id as STRING) as f_user_id,f_device_type,f_home_id,
         |cast(f_device_id as STRING) as f_device_id,f_login_time,
         |f_logout_time
         |from t_user_online_history where day='$day'
       """.stripMargin)
  }

  /**
    * 时间区间拆分为半小时一片
    *
    * @param day        计算哪天的
    * @param start_time 开始时间
    * @param end_time   结束时间
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
    * 按天计算历史登陆用户总数
    * @author lizhy@20181220
    * @param date
    * @param sqlContext
    */
  def hisOnlineUserCount(date:String,sc:SparkContext,sqlContext:SQLContext):DataFrame={
    val asDateTime = DateUtils.dateStrToDateTime(date,DateUtils.YYYYMMDD).plusDays(1).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    //added@20190519
    val querySql = clusterRegion match {
      case "320200" => { //无锡
        s"( select * from table where f_create_time>'2019-05-15 00:00:00' and f_create_time<'$asDateTime') t"
      }
      case _ => { //默认
        s"( select * from table where f_create_time<'$asDateTime') t"
      }
    }
    val tokenDF = MultilistUtils.getMultilistData("homed_iusm","account_token",sqlContext,querySql)

    val historyTokenDF = sqlContext.sql("select * from bigdata.account_token  ")
     historyTokenDF.unionAll(tokenDF.select(historyTokenDF.columns.map(col):_*))
       .registerTempTable("account_token")

    val accontInfoSql = s"(SELECT home_id ,da from account_info where STATUS=1 and create_time < '$asDateTime' ) b"
    val accontInfoDF = sqlHB.loadMysql(sqlContext, accontInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    accontInfoDF.registerTempTable("account_info")

    val addressInfoSql = s"(SELECT  home_id, region_id from address_info ) c"
    val addressInfoDF = sqlHB.loadMysql(sqlContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    addressInfoDF.registerTempTable("address_info")

    val cntDf = clusterRegion match {
      case "340200" => {
        UsersProcess.hisOnlineUserCount(date,sqlContext)
      }
      case _ => { //默认
        sqlContext.sql(
          s"""
             |select '$date' as h_date, a.device_type as h_device_type,
             | nvl(c.region_id,0) AS h_region_id,
             | COUNT(DISTINCT b.home_id) AS f_hist_user_count
             |from account_token  a
             |inner join  account_info  b on a.da = b.da
             |left  join  address_info c  on b.home_id = c.home_id
             |group by a.device_type,nvl(c.region_id,0)
             |
      """.stripMargin)
      }
    }
//    val cntDf = sqlHB.loadMysql(sqlContext, cntSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val unknownRgDf = cntDf.filter("h_region_id = 0")
      .selectExpr("h_date","h_device_type","h_region_id","'unknown' as h_region_name","0 as h_city_id",
        "'unknown' as h_city_name","0 as h_province_id ","'unknown' as h_province_name","f_hist_user_count")
    val regionDf = getRegionDf(sc,sqlContext)
    val regDf = cntDf.join(regionDf,regionDf("h_area_id")===cntDf("h_region_id"),"inner")
      .selectExpr("h_date","h_device_type","h_region_id","nvl(h_region_name,'unknown') as h_region_name","nvl(h_city_id,0) as h_city_id",
        "nvl(h_city_name,'unknown') as h_city_name","nvl(h_province_id,0) as h_province_id ","nvl(h_province_name,'unknown') as h_province_name","f_hist_user_count")
    regDf.unionAll(unknownRgDf)
  }
  /**
    * 区域信息
    * @param sc
    * @param sqlContext
    */
  def getRegionDf(sc:SparkContext,sqlContext:SQLContext)={
    //项目部署地code(省或者地市)
    val regionCode = RegionUtils.getRootRegion
    val regionBC: Broadcast[String] = sc.broadcast(regionCode)
    //区域信息表
    val region =
      s"""
         |(select a.area_id  h_area_id,a.area_name as h_region_name,
         | a.city_id  h_city_id,c.city_name as h_city_name,
         | c.province_id h_province_id,p.province_name as h_province_name
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |where a.city_id=${regionBC.value} or c.province_id=${regionBC.value}
         |) as region
        """.stripMargin
    DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }
}
