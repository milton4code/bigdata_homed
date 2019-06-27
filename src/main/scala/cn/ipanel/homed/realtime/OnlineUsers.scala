package cn.ipanel.homed.realtime

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.joda.time.DateTime

/**
  * 统计在线用户数 created by lizhy@20190307
  */
object OnlineUsers {
  def main(args: Array[String]): Unit = {
    println("实时在线用户统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    val sparkSession: SparkSession = SparkSession("OnlineUsersRealtime")
    val sc: SparkContext = sparkSession.sparkContext
    val hiveContext: HiveContext = sparkSession.sqlContext
    var duration = 2
    var partAmt = 200
    try{
      if(args.length == 2){
        duration = args(0).toInt
        partAmt = args(1).toInt
      }else if(args.length == 1){
        duration = args(0).toInt
      }
    }catch {
      case e:Exception => {
        println("输入参数无效,按默认变量值执行程序！")
        e.printStackTrace()
      }
    }
    val nodeTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
    countOnlineUsers(sparkSession,hiveContext,duration,partAmt,nodeTime)
    println("实时在线用户统计结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    sparkSession.stop()
  }

  /**
    * 计算在线用户数
    * @param sqlContext
    * @param duration
    * @param partAmt
    */
  def countOnlineUsers(sparkSession: SparkSession,sqlContext:HiveContext,duration:Int,partAmt:Int,nodeTime:String)={
    val sc = sparkSession.sparkContext
    val nodeTimeBC = sc.broadcast(nodeTime)
    val userRegionDf = getUserRegion(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //配置区域信息
//    val configDf = UserRegion.getConfigRegion(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val runlogDf = getRunLogOnlineUserDf(sqlContext)
    val userlogDf = getUserLogOnlineUserDf(sqlContext)
    val userJoinRegionDf = getOnlineUsersDf(sqlContext,runlogDf,userlogDf)
        .join(userRegionDf,runlogDf("f_user_id")===userRegionDf("r_user_id"),"inner")
    val onlineUserDf = userJoinRegionDf.groupBy("f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_name","r_region_id","f_service_type")
        .count().withColumnRenamed("count","f_user_count")
        .selectExpr(s"'${nodeTimeBC.value}' as f_date_time","f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_name"
          ,"r_region_id as f_region_id","f_service_type","f_user_count")
    try{
//      onlineUserDf.show(100)
      println("插入记录数：" + onlineUserDf.count())
      DBUtils.saveToMysql(onlineUserDf, Tables.T_USER_COUNT_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存最新用户数
      if(!onlineUserDf.rdd.isEmpty()){
        NodeTime.updateTimeNode(nodeTime,"user") //更新节点信息
      }
      NodeTime.deleteHistRealtimeInfo(nodeTime,"user") //删除历史实时在线用户数统计信息
    }catch {
      case e:Exception =>{
        println("保存用户数失败！")
        e.printStackTrace()
      }
    }finally {
//      configDf.unpersist()
      userRegionDf.unpersist()
      sqlContext.clearCache()
    }
  }

  /**
    * run log在线用户
    * @param sqlContext
    */
  def getRunLogOnlineUserDf(sqlContext:HiveContext)={
    val runlogSql =
      s"""
        |(select f_user_id,f_terminal,f_region_id,f_service_type,f_update_time
        |from ${Tables.T_RUNLOG_USER_STATUS_REALTIME}
        |where f_online_status = 1) as run_log
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext,runlogSql)
  }

  /**
    * 用户上报在线用户
    * @param sqlContext
    */
  def getUserLogOnlineUserDf(sqlContext:HiveContext)={
    val userlogSql =
      s"""
         |(select f_user_id,f_terminal,f_region_id,f_service_type,f_update_time
         |from ${Tables.T_USERLOG_USER_STATUS_REALTIME}
         |where f_online_status = 1) as user_log
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext,userlogSql)
      .selectExpr("f_user_id as u_user_id","f_terminal as u_terminal","f_region_id as u_region_id"
        ,"f_service_type as u_service_type","f_update_time as u_update_time")
  }
  /**
    * 获取区域信息
    */
  def getUserRegion(sqlContext: HiveContext): DataFrame = {
    val userUsersql =
      s"""
         |(select f_user_id,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name
         | from ${Tables.T_USER_REGION}) as user_region
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext,userUsersql)
      .withColumnRenamed("f_user_id","r_user_id")
      .withColumnRenamed("f_region_id","r_region_id")
  }


  def getOnlineUsersDf(sqlContext:HiveContext,runlogUserDf:DataFrame,userlogUserDf:DataFrame) ={
    runlogUserDf.join(userlogUserDf,runlogUserDf("f_user_id")===userlogUserDf("u_user_id"),"left_outer")
      .selectExpr(s"f_user_id","f_terminal","f_region_id"
        ,"case when f_service_type = '0' and u_service_type is not null then u_service_type " +
          "when f_service_type = '0' and u_service_type is null then 'other'" +
          "when f_service_type != '0' then f_service_type end as f_service_type")
  }
}
