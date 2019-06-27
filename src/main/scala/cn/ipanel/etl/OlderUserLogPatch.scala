package cn.ipanel.etl

import cn.ipanel.common.{Constant, DBProperties, Tables}
import cn.ipanel.customization.wuhu.users.UsersProcess
import cn.ipanel.etl.MysqlToHive.getDistinctUser
import cn.ipanel.utils.DBUtils.loadMysql
import cn.ipanel.utils.DateUtils.transformDateStr
import cn.ipanel.utils.{DateUtils, RegionUtils, UserUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * 老版用户离线日志处理补丁
  */
object OlderUserLogPatch {
  val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码 added @20190421
  def process (hiveContext: HiveContext, day:String, partitions:Int=30): Unit ={

    //系统默认partition路径/user/hive/warehouse/
    hiveContext.sql("use bigdata")
    val  nextDay = DateUtils.getAfterDay(day)
    hiveContext.sql(s"alter table t_user_online_history add if not exists partition (day='$day')")

    val day2=transformDateStr(day)
    val offLineHistorySql = s"(SELECT * FROM t_user_online_history where  f_logout_time  >= $day  and f_login_time<$nextDay) t"
    val onlineSql =
      s"""
         | (SELECT f_user_id,f_device_id,f_device_type,f_home_id,
         | f_srvtype_id,f_login_time,'$day2 23:59:59' as f_logout_time,f_ipaddress,f_port,f_region_id
         | FROM t_user_online_statistics
         | where f_login_time<$nextDay) t
       """.stripMargin
    val offLineHistoryDF=loadMysql(hiveContext, offLineHistorySql, DBProperties.JDBC_URL_IACS, DBProperties.USER_IACS, DBProperties.PASSWORD_IACS)
    val onlineDF=loadMysql(hiveContext, onlineSql, DBProperties.JDBC_URL_IACS, DBProperties.USER_IACS, DBProperties.PASSWORD_IACS)

    val savedDF1 = offLineHistoryDF.unionAll(onlineDF)
      .selectExpr(" f_user_id", " f_device_id","f_device_type"," f_home_id","f_srvtype_id",
        "f_login_time"," f_logout_time"," f_ipaddress"," f_port"," substr(f_region_id,1,6) as f_region_id"
        ,s"'$day' day ")
    val savedDF = clusterRegion match {
      case "340200" => { //芜湖
        UsersProcess.getEffectOnlineUserDf(day,savedDF1,hiveContext)
      }
      case _ => savedDF1
    }
    savedDF.persist()
    val distinctUserDF = getDistinctUser(savedDF,hiveContext,day)

    savedDF.repartition(partitions).write.format("orc").mode(SaveMode.Overwrite).partitionBy("day").insertInto(Tables.T_USER_ONLINE_HISTORY)
    distinctUserDF.repartition(partitions).write.format("orc").mode(SaveMode.Overwrite).partitionBy("day").insertInto(Tables.ORC_LOGIN_USER)
    savedDF.unpersist()

  }
}
