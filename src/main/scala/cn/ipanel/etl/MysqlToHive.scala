package cn.ipanel.etl

import cn.ipanel.common.{Constant, DBProperties, SparkSession, Tables}
import cn.ipanel.customization.wuhu.users.UsersProcess
import cn.ipanel.homed.general.OnlineUser.clusterRegion
import cn.ipanel.utils.MultilistUtils.getHomedVersion
import cn.ipanel.utils.{DBUtils, DateUtils, UserUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * mysql数据按天分区导入hive
  * 20180208 create by wenbin
  * */
object MysqlToHive{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("MysqlToHive")
    val hiveContext = sparkSession.sqlContext
    val sc=sparkSession.sparkContext

    if (args.length != 1) {
      println("请输入日期 例如 20180518")
      sys.exit(-1)
    }
    val  day = args(0)
    process(day,20,hiveContext)
    sc.stop()
  }

  def process(day:String,partitions:Int=20,hiveContext:HiveContext): Unit ={
    val homedVersion = getHomedVersion()
    if( homedVersion != 0.0F && Constant.OLDER_HOMED_OVERION  >= homedVersion ){
      OlderUserLogPatch.process(hiveContext,day,partitions)
    }else{

      val nextDay = DateUtils.getAfterDay(day)
      hiveContext.sql("use bigdata")
      val onlineUserDF = getOnlineUser(day, nextDay, hiveContext)
      val offlineUserDF = getOfflineUser(day, hiveContext)
      val savedDF1 = onlineUserDF.unionAll(offlineUserDF)
        .selectExpr(" f_user_id", " f_device_id","f_device_type"," f_home_id","f_srvtype_id",
          "f_login_time"," f_logout_time"," f_ipaddress"," f_port"," substr(f_region_id,1,6) as f_region_id"
          ,s"'$day' day ")
      val savedDF = clusterRegion match {
        case "340200" => { //芜湖
          UsersProcess.getEffectOnlineUserDf(day,savedDF1,hiveContext)
        }
        case _ => savedDF1
      }
      val distinctUserDF = getDistinctUser(savedDF,hiveContext,day)
//      distinctUserDF.show()
      distinctUserDF.repartition(partitions).write.format("orc").mode(SaveMode.Overwrite).partitionBy("day").insertInto(Tables.ORC_LOGIN_USER)
      savedDF.repartition(partitions).write.format("orc").mode(SaveMode.Overwrite).partitionBy("day").insertInto(Tables.T_USER_ONLINE_HISTORY)
    }

    println("-----MysqlToHive----佛祖保佑----------")

  }



  /**
    * 获取去重 用户
    */
  def getDistinctUser(df:DataFrame,hiveContext:SQLContext,day:String):DataFrame={
    df.registerTempTable("t_hive")
    val userDF = hiveContext.sql(
      s"""
         |select distinct(f_user_id ) user_id ,f_region_id as region_id ,max(f_login_time) login_time, '$day' as day,
         |f_device_type as terminal
         |from t_hive
         |group by f_user_id,f_region_id,f_device_type
      """.stripMargin)
    val areaDF = hiveContext
      .sql("select province_id,province_name,city_id,city_name,area_id as region_id ,area_name as region_name  from bigdata.orc_area")
    /*val userDF =
      clusterRegion match {
        case "340200" => { //芜湖
          UsersProcess.getEffectOnlineUserDf(day,userDF1,hiveContext)
        }
        case _ => userDF1 //默认不处理
      }*/
    userDF.join(areaDF, Seq("region_id"))
      .select("user_id","province_id","province_name","city_id","city_name","region_id","region_name","login_time","terminal","day")
  }

  private def getOnlineUser(day: String, nextDay: String, hiveContext: SQLContext): DataFrame = {
    val day2 = DateUtils.transformDateStr(day)
    val sql =
      s"""( SELECT f_user_id,f_device_id,f_device_type,f_home_id,
         | f_srvtype_id,f_login_time,'$day2 23:59:59' as f_logout_time,f_ipaddress,f_port,substr(f_region_id,1,6) f_region_id
         | FROM t_user_online_statistics
         | where f_login_time<$nextDay) t
          """.stripMargin
    DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_IACS, DBProperties.USER_IACS, DBProperties.PASSWORD_IACS)
  }

  private def getOfflineUser(day: String, hiveContext: SQLContext): DataFrame = {
    hiveContext.sql(
      s"""
         |SELECT exts['UserID'] f_user_id, exts['DeviceID'] f_device_id,exts['DeviceType'] f_device_type,
         |exts['HomeID'] f_home_id, exts['SrvTypeID'] f_srvtype_id, exts['LoginTime'] f_login_time,
         | exts['LogoutTime']  f_logout_time,exts['IPAddr'] f_ipaddress,
         | exts['Port'] f_port,substr(exts['RegionID'],1,6) f_region_id
         | FROM bigdata.orc_iacs  where day= $day and key_word='UserOffline'
      """.stripMargin)
  }
}
