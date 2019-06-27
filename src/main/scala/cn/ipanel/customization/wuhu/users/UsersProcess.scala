package cn.ipanel.customization.wuhu.users

import cn.ipanel.common.{DBProperties, Tables, UserRegion}
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
  * 芜湖 用户相关计算
  * @author lizhy@20190421
  */
object UsersProcess {
//  val effectedDateTime = "2019-04-08 00:00:00"
//  val effectedDate = "20190408"
  val effectedStatus = "1"

  /**
    * 匹配有效home
    * @param sourceDaDf
    * @param nextDate
    * @param sqlContext
    * @return
    */
  def matchEffectPayHomeDf(sourceDaDf:DataFrame,nextDate:String,sqlContext:SQLContext):DataFrame={
    val nextDateTime = DateUtils.dateStrToDateTime(nextDate,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val effPayHomeDf = UsersProcess.getEffectPayHome(nextDateTime,sqlContext)
    sourceDaDf.join(effPayHomeDf,sourceDaDf("home_id")===effPayHomeDf("target_id"),"inner").drop("target_id")
  }
  /**
    * 获取有效付费套餐homed id
    * @20190604
    * @param nextDateTime
    */
  def getEffectPayHome(nextDateTime:String,sqlContext:SQLContext) ={
    println("芜湖定制化付费套餐home..")
    val querySql =
      s"""
        |(select distinct(target_id) as target_id from table where buy_time<'$nextDateTime' and charge_id in (27,28,31,32,24,34,23,41,35,12,45,40,39,37,38,25,29,30)) as et
      """.stripMargin
    val effectPayHome = MultilistUtils.getMultilistData("homed_iusm","user_pay_info",sqlContext,querySql)
    effectPayHome
  }

  /**
    *
    * @param nextDate
    * @return
    */
  @deprecated
  def getEffectStr(nextDate:String)={
    val nextDateTime = DateUtils.dateStrToDateTime(nextDate,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val effAccWhereStr = " status = " + effectedStatus +
      " AND home_id in (select distinct(target_id) from user_pay_info where buy_time < '" +
      nextDateTime +"' and charge_id in (27,28,31,32,24,34,23,41,35,12,45,40,39,37,38,25,29,30))"
    effAccWhereStr
  }

  /**
    *
    * @param date
    * @param sqlContext
    * @return
    */
  def getEffectedUserDf(date:String,sqlContext:SQLContext)= {
    val nextDateTime = DateUtils.dateStrToDateTime(date,DateUtils.YYYYMMDD).plusDays(1).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val usrSql =
      s"""
         |(SELECT cast(DA as char) as da, cast(home_id as char) as home_id, nick_name, mobile_no FROM ${Tables.T_ACCOUNT_INFO}
         |WHERE status = $effectedStatus and create_time<'$nextDateTime'
         |) as usr
       """.stripMargin
    val payHomeDf = getEffectPayHome(nextDateTime,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("payHomeDf count:" + payHomeDf.count())
    val userDf = DBUtils.loadMysql(sqlContext,usrSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    userDf.join(payHomeDf,userDf("home_id")===payHomeDf("target_id"),"inner").drop("target_id")
  }
  def hisOnlineUserCount(date:String,sqlContext:SQLContext):DataFrame={
    val defaultRegion = UserRegion.getRegionCode()
    val effectUsrDf = getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val histOnlineDf =
      sqlContext.sql(
        s"""
         |select f_user_id,f_device_id,f_device_type,f_home_id,substr(f_region_id,1,6) as f_region_id
         |from ${Tables.T_USER_ONLINE_HISTORY}
         |where day<='$date'
       """.stripMargin)
    histOnlineDf.join(effectUsrDf,histOnlineDf("f_user_id")===effectUsrDf("da"),"inner").registerTempTable("t_hist_online")
    sqlContext.sql(
      s"""
         |select '${date}' as h_date,f_device_type as h_device_type,nvl(f_region_id,'${defaultRegion}') AS h_region_id,count(distinct f_user_id) as f_hist_user_count
         |from t_hist_online
         |group by f_device_type,nvl(f_region_id,'${defaultRegion}')
         """.stripMargin)
  }
  //设备维度统计开机统计开机数
  def loginDevDf(date:String,sqlContext:SQLContext,sourceDf:DataFrame)={
    val effUserDf = getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //f_user_id,f_device_type,f_home_id,f_device_id,f_login_time,f_logout_time
    sourceDf.join(effUserDf,sourceDf("f_user_id")===effUserDf("da"),"inner")
      .selectExpr("f_user_id","f_device_type","f_home_id","f_device_id","f_login_time","f_logout_time")
  }
  //有效家庭home帐号
  def getEffectHomeDf(nextDate:String,sqlContext: SQLContext)={
    val nextDateTime = DateUtils.dateStrToDateTime(nextDate,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val effHomeSql =
      s"""
         |(SELECT home_id ,master_account as DA,status,date_format(f_create_time,'%Y%m%d %H:%i:%s') as f_create_time,
         | date_format(f_status_update_time,'%Y%m%d %H:%i:%s') as f_status_update_time
         | from home_info
         | WHERE f_create_time < '$nextDateTime'
         | ) as t_home_info
       """.stripMargin
    val payHomeDf = getEffectPayHome(nextDateTime,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("payHomeDf count:" + payHomeDf.count())
    val effHomeDf = DBUtils.loadMysql(sqlContext,effHomeSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    effHomeDf.join(payHomeDf,effHomeDf("home_id")===payHomeDf("target_id"),"inner").drop("target_id")
  }
  //有效开机用户
  def getEffectOnlineUserDf(date:String,userDf:DataFrame,sqlContext: SQLContext)={
    val effUserDf = getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    userDf.join(effUserDf,userDf("f_user_id")===effUserDf("da"),"inner")
      .where(s"day <= '$date'")
      .selectExpr("f_user_id","f_device_id","f_device_type","f_home_id","f_srvtype_id","f_login_time"," f_logout_time"," f_ipaddress"," f_port"," substr(f_region_id,1,6) as f_region_id","day")
  }

}
