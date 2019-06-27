package cn.ipanel.monitor

import java.io.{File, PrintWriter}

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.SQLContext

/**
  * 初步监控程序运行 [初版]
  * 1.程序运行时长  主要读取指定目录下的log日志
  * 2.程序运行成功还是失败: 主要读取相关模块表中的数据是否完整
  * 3.监控集群spark ,hbase集群是否工作正常
  */
object MonitorApp {

  def main(args: Array[String]): Unit = {
    val session = SparkSession("MonitorApp")
    if(args.length != 1){
      println("参数错误,请输入日期,例如 20190101")
      sys.exit(-1)
    }

    val sqlContext = session.sqlContext
    val day = args(0)
    //    val cluster = args(1)
    var content = new StringBuilder()
    val separator = System.getProperty("line.separator")
    content.append("[集群大数据监控]").append(separator)
    content.append(getUserCount(sqlContext, day)).append(separator) //用户
    content.append(getOlineUser(sqlContext, day)).append(separator) //在线用户
    content.append(getBusinessIncome(sqlContext, day)).append(separator) //业务营收
    content.append(terminalLive(sqlContext, day)).append(separator) //直播
    content.append(terminalLookback(sqlContext, day)).append(separator) //回看
    content.append(terminalDemand(sqlContext, day)).append(separator) //点播
    //    content.append(terminalColumn(sqlContext, day)).append(separator)
    content.append(mediaStock(sqlContext, day)).append(separator) //媒资库存
    content.append(reportOperatorSummary(sqlContext, day)).append(separator) //运营汇总
    //    content.append(reportActiveDemandUser(sqlContext, day)).append(separator)
    content.append(reportHotWord(sqlContext, day)).append(separator) //热搜词
    content.append(reportPersonal(sqlContext, day)).append(separator) //个人开户
    content.append(reportFamily(sqlContext, day)).append(separator) //家庭开户
    content.append(reportRevenue(sqlContext, day)).append(separator) //业务营收
    content.append(cpSpReport(sqlContext,day)).append(separator) //内容提供商
    val writer = new PrintWriter(new File("/r2/bigdata/bigdata_homed/logs/monitor" + day + ".log"))
    writer.write(content.toString())
    writer.flush()
    writer.close()

  }

  //t_personal_open_account


  /**
    * 统计表报 营收报表(销售量报表)
    *
    */
  def reportRevenue(sqlContext: SQLContext, day: String): String = {
    val tarDate = DateUtils.dateStrToDateTime(day,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
    val sql1 = s"(select f_order_time from t_revenue_report where f_order_time like '$tarDate" + "%' ) t"
    var content = "[统计报表] 营收报表,执行失败!"
    try {
      val flag1 = loadMysql(sqlContext, sql1)
      if (flag1) {
        content = "[统计报表] 营收报表,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }


  /**
    * 统计表报 家庭开户报表
    *
    */
  def reportFamily(sqlContext: SQLContext, day: String): String = {
    val tarDate = DateUtils.dateStrToDateTime(day,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
    val sql1 = s"(select f_open_account_time from t_home_open where f_open_account_time like '$tarDate" + "%') t"
    var content = "[统计报表] 家庭开户报表,执行失败!"
    try {
      val flag1 = loadMysql(sqlContext, sql1)
      if (flag1) {
        content = "[统计报表] 家庭开户报表,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  /**
    * 统计表报 个人开户报表
    * PersonalOpenAccount
    */
  def reportPersonal(sqlContext: SQLContext, day: String): String = {
    val tarDate = DateUtils.dateStrToDateTime(day,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
    val sql1 = s"(select f_open_account_time as cnt from t_personal_open_account where f_open_account_time like '$tarDate" + "%' ) t"
    var content = "[统计报表] 个人开户报表,执行失败!"
    try {
      val flag1 = loadMysql(sqlContext, sql1)
      if (flag1) {
        content = "[统计报表] 个人开户报表,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  /**
    * 统计表报 热搜词
    * SearchDetail
    */
  def reportHotWord(sqlContext: SQLContext, day: String): String = {
    val sql1 = s"(select f_date from t_user_search_keyword where f_date='$day') t"
    var content = "[统计报表] 热搜词,执行失败!"
    try {
      //      val flag1 = loadFromPhoenix(sqlContext, sql1)
      val flag1 = loadMysql(sqlContext, sql1)
      if (flag1) {
        content = "[统计报表] 热搜词,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }


  /**
    * 统计表报 点播活跃用户
    */
  def reportActiveDemandUser(sqlContext: SQLContext, day: String): String = {
    val mounth_day = day.substring(0, 6)
    //运营汇总统计
    val sql1 = s"(select F_DATE  from t_active_demand_user_by_day where f_date='$day') as t1"
    //导出报表
    val sql2 = s"(select F_DATE  from t_active_demand_user_by_month where f_date='$mounth_day') as t2"
    var content = "[统计报表] 点播活跃用户,执行失败!"
    try {
      val flag1 = loadFromPhoenix(sqlContext, sql1)
      val flag2 = loadFromPhoenix(sqlContext, sql2)
      if (flag1 && flag2) {
        content = "[统计报表] 点播活跃用户,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }
  /**
    * 终端统计 内容提供商统计
    */
  def cpSpReport(sqlContext: SQLContext, day: String): String = {
    val mounth_day = day.substring(0, 6)
    //内容提供商排行
    val sql1 = s"(select F_DATE  from t_demand_cp_sp_rank where f_date='$day') as t1"
    //导出报表
    var content = "[终端统计] 内容提供商统计失败!"
    try {
      val flag1 = loadFromPhoenix(sqlContext, sql1)
      if (flag1) {
        content = "[终端统计] 内容提供商统计成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  /**
    * 报表 运营汇总
    * BusinessVisitCount
    */
  def reportOperatorSummary(sqlContext: SQLContext, day: String): String = {
    //运营汇总统计
    //    val sql1 = s"(select count(1) as cnt from t_service_visit_user_top_rank where f_date='$day') t"
    //导出报表
    val sql2 = s"(select F_DATE as cnt from t_service_visit_users where f_date='$day') as t"
    var content = "[统计报表] 运营汇总,执行失败!"
    try {
      //      val flag1 = loadMysql(sqlContext, sql1)
      val flag2 = loadFromPhoenix(sqlContext, sql2)
      //      if (flag1 && flag2) {
      if (flag2) {
        content = "[统计报表] 运营汇总,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  /**
    * 媒资库存
    * DemandMeizi
    */
  def mediaStock(sqlContext: SQLContext, day: String): String = {
    //媒资库存  点播
    //    val sql1 = s"(select f_date as cnt from t_meizi_statisc_report where f_date='$day') t"
    val date = DateUtils.dateStrToDateTime(day,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
    //回看
    val sql2 = s"(select f_start_time as cnt from t_meizi_lookback_report where f_start_time >='$date 00:00:00' and f_start_time <= '$date 23:59:59' ) t"
    var content = "[媒资库存] 点播,回看功能执行失败!"
    try {
      val flag1 = loadMysql(sqlContext, sql2)
      //      val flag2 = loadMysql(sqlContext, sql2)
      //      if (flag1 && flag2) {
      if (flag1) {
        content = "[媒资库存] 点播,回看功能执行成功 !"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }


  /**
    * 终端统计 栏目
    * ColumnDetailNew
    */
  def terminalColumn(sqlContext: SQLContext, day: String): String = {
    val month_day = day.substring(0, 6)
    //人数走势  按天  DemandUser
    val sql1 = s"(select F_DATE  from t_column_report_by_day where F_DATE='$day') as t1"
    //排行榜 按月  DemandReportPeriod
    val sql2 = s"(select F_DATE  from t_column_report_by_month   where f_date='$month_day') as t2"
    var content = "栏目模块 ,执行失败"
    try {
      val flag1 = loadFromPhoenix(sqlContext, sql1)
      val flag2 = loadFromPhoenix(sqlContext, sql2)
      if (flag1 && flag2) {
        content = "栏目模块,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }


  /**
    * 终端统计 点播
    * DemandUser
    * DemandReportPeriod
    */
  def terminalDemand(sqlContext: SQLContext, day: String): String = {
    val month_day = day.substring(0, 6)
    //人数走势  按天  DemandUser
    val sql1 = s"(select F_DATE from t_demand_user_by_day where F_DATE='$day') as t1"
    //排行榜 按月  DemandReportPeriod
    val sql2 = s"(select F_DATE from t_demand_report_by_month where f_date='$month_day') as t2"
    var content = "[终端统计]点播在线走势,排行榜任务,执行失败"
    try {
      val flag1 = loadFromPhoenix(sqlContext, sql1)
      val flag2 = loadFromPhoenix(sqlContext, sql2)
      if (flag1 && flag2) {
        content = "[终端统计] 点播在线走势,排行榜任务 ,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  /**
    * 终端统计 回看
    * LookbackUser
    * LookbackReportPeriod
    */
  def terminalLookback(sqlContext: SQLContext, day: String): String = {
    val month_day = day.substring(0, 6)
    //人数  按天
    val sql1 = s"(select F_DATE from t_look_user_by_day where F_DATE='$day') as t1"
    //排行榜 按月
    val sql2 = s"(select F_DATE from t_lookback_report_by_month   where f_date='$month_day') as t2"
    var content = "[终端统计] 回看在线走势,排行榜任务,执行失败"
    try {
      val flag1 = loadFromPhoenix(sqlContext, sql1)
      val flag2 = loadFromPhoenix(sqlContext, sql2)
      if (flag1 && flag2) {
        content = "[终端统计] 回看在线走势,排行榜任务,执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  //终端统计,直播
  def terminalLive(sqlContext: SQLContext, day: String): String = {
    val month_day = day.substring(0, 6)
    //按天
    val sql1 = s"(select F_DATE from t_live_channel_by_day where F_DATE='$day') as t1"
    //按月
    val sql2 = s"(select F_DATE from t_live_channel_by_month where f_date='$month_day') as t2"
    var content = "[直播模块] 频道统计执行失败"
    try {
      val flag1 = loadFromPhoenix(sqlContext, sql1)
      val flag2 = loadFromPhoenix(sqlContext, sql2)
      if (flag1 && flag2) {
        content = "[直播模块] 频道统计执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  /**
    * 业务营收  BusinessRevenue
    * 整体营收
    * cs/sp营业额
    */
  private def getBusinessIncome(sqlContext: SQLContext, day: String): String = {
    //整体营收 第一张表
    val daSql = s"(select f_date  from t_business_operation_revenue_region where f_date='$day') t"
    //cp/sp 最后一张表
    //    val userPaySql = s"(select f_date from t_unsubscribe_by_day   where f_date='$day') t"
    var content = "[业务营收] 整体营收,cp/sp 执行失败"
    try {
      val flag1 = loadMysql(sqlContext, daSql)
      //      val flag2 = loadMysql(sqlContext, userPaySql)
      if (flag1) {
        //        if (flag1 && flag2) {
        content = "[业务营收] 整体营收,cp/sp 执行成功!"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }

  //开机用户
  private def getOlineUser(sqlContext: SQLContext, day: String): String = {
    //用户总量 按个人统计
    val daSql = s"(select f_date  from t_da where f_date='$day') t"
    var content = "[开机用户] 执行失败"
    try {
      val flag = loadMysql(sqlContext, daSql)
      if (flag) {
        content = "[开机用户] 执行成功"
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    content

  }

  /**
    *
    * 用户总量和付费用户
    */
  def getUserCount(sqlContext: SQLContext, day: String): String = {
    //用户总量 按个人统计
    val daSql = s"(select f_date from t_online_dev_by_day where f_date='$day') t"
    //付费用户
    val userPaySql = s"(select f_date from t_home_by_pay where f_date='$day') t"
    var content = "[用户总量] 付费用户执行失败"
    try {
      val flag1 = loadMysql(sqlContext, daSql)
      val flag2 = loadMysql(sqlContext, userPaySql)
      if (flag1 && flag2) {
        content = "[用户总量] 付费用户执行成功"
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    content
  }


  /**
    * 根据查询结果返回值判断任务成功或失败
    * 大于0 则成功, 否则失败
    * 单表查询
    */
  def loadMysql(sqlContext: SQLContext, tableName: String,
                url: String = DBProperties.JDBC_URL,
                user: String = DBProperties.USER,
                password: String = DBProperties.PASSWORD): Boolean = {
    val df = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> user,
        "password" -> password,
        "dbtable" -> tableName)).load()
    val result = df.count()
    val flag = if (result > 0) true else false
    flag
  }

  /**
    *
    * @param sqlContext
    * @param sql
    * @return
    */
  def loadFromPhoenix(sqlContext: SQLContext, sql: String): Boolean = {
    val df = DBUtils.loadDataFromPhoenix2(sqlContext, sql)
    val result = df.count()
    val flag = if (result > 0) true else false
    flag

  }


}
