package cn.ipanel.homed.realtime

import java.sql.{DriverManager, PreparedStatement, ResultSet}

import cn.ipanel.common.{DBProperties, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

object NodeTime {
  /**
    * 更新节点信息 created by lizhy@20190307
    * @param nodeTime
    * @param nodeType
    */
  def updateTimeNode(nodeTime:String,nodeType:String): Unit ={
    val lastDateTime = DateUtils.getYesterday(DateUtils.YYYY_MM_DD_HHMM)
    val nowDateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
    val lastTopDateTimeList = getLastTopNodeTime(nodeType,2)
    var keepDateTimeStr = " "
    if(!lastTopDateTimeList.isEmpty){
      lastTopDateTimeList.foreach(x => {
        keepDateTimeStr = keepDateTimeStr + s"and f_date_time != '" + x + "' "
      })
    }
    val deleteNodeStr = nodeType match {
      case "user" => s"(f_date_time < '$lastDateTime' or not f_date_time like '%:00') and f_date_time != '${nodeTime}' " + keepDateTimeStr //在线用户保当前、前一个点和24小时历史整点
      case "live" => s"f_date_time < '$lastDateTime'" //直播保留24小时内历史数据和当前数据
      case "channelType" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}' "+ keepDateTimeStr //直播类型保存当前和24小时历史整点
      case "demand" => s"f_date_time != '${nodeTime}' "+ keepDateTimeStr  //点播只保留当前数据
      case "demandContentType" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}'" + keepDateTimeStr //点播类型保存当前和24小时历史整点
      case "lookback" => s"f_date_time != '${nodeTime}'" + keepDateTimeStr //回看只保留当前数据
      case "lookbackContentType" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}'" + keepDateTimeStr //回看类型保存当前和24小时历史整点
      case _ => "unknown"
    }
    //增加当前节点
    val addNodeSql =
      s"""
         |insert into ${Tables.T_TIMENODE_REALTIME}(f_date_time,f_update_time,f_node_type)
         |values ('$nodeTime','$nowDateTime','$nodeType')
      """.stripMargin
    //删除历史节点
    val deleteNodeSql =
      s"""
         |delete from ${Tables.T_TIMENODE_REALTIME}
         |where f_node_type = '${nodeType}' and ${deleteNodeStr}
      """.stripMargin
    try{
//      println("deleteNodeSql:" + deleteNodeSql)
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, addNodeSql)
    }catch {
      case e:Exception => {
        println("增加时间节点失败，时间：" + nodeTime + "，类型："+ nodeType)
        e.printStackTrace()
      }
    }
    try{
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, deleteNodeSql)
      println("删除时间节点：" + nodeTime + "，类型："+ nodeType)
    }catch{
      case e:Exception => {
        println("删除时间节点失败，时间：" + nodeTime + "，类型："+ nodeType)
        e.printStackTrace()
      }
    }

  }

  /**
    * 删除历史统计信息（一般为24小时前）
    * @param nodeTime 当前节点
    * @param nodeType 节点类型
    */
  def deleteHistRealtimeInfo(nodeTime:String,nodeType:String): Unit ={
    val lastDateTime = DateUtils.getYesterday(DateUtils.YYYY_MM_DD_HHMM)
    val nowDateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
    val lastNodeTime = getLastNodeTime(nodeType)
    val countTableName = nodeType match {
      case "user" => Tables.T_USER_COUNT_REALTIME
      case "live" => Tables.T_CHNN_LIVE_REALTIME
      case "channelType" => Tables.T_CHANNEL_TYPE_PLAYTIME_REALTIME
      case "demand" => Tables.T_PROGRAM_DEMAND_REALTIME
      case "demandContentType" => Tables.T_DEMAND_CONTENT_TYPE_REALTIME
      case "lookback" => Tables.T_PROGRAM_LOOKBACK_REALTIME
      case "lookbackContentType" => Tables.T_PROGRAM_LOOKBACK_TYPE_REALTIME
      case _ => "unknown"
    }
    val lastTopDateTimeList = getLastTopNodeTime(nodeType,2)
    var keepDateTimeStr = ""
    if(!lastTopDateTimeList.isEmpty){
      lastTopDateTimeList.foreach(x => {
        keepDateTimeStr = keepDateTimeStr + s"and f_date_time != '" + x + "' "
      })
    }
    val deleteNodeStr = nodeType match {
      case "user" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}' "+ keepDateTimeStr //在线用户保当前和24小时历史整点
      case "live" => s"f_date_time < '${lastDateTime}'" //直播保留24小时内历史数据和当前数据
      case "channelType" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}' " + keepDateTimeStr //直播类型保存当前和24小时历史整点
      case "demand" => s"f_date_time != '${nodeTime}'" + keepDateTimeStr //点播只保留当前数据
      case "demandContentType" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}'" + keepDateTimeStr //点播类型保存当前和24小时历史整点
      case "lookback" => s"f_date_time != '${nodeTime}'" + keepDateTimeStr //回看只保留当前数据
      case "lookbackContentType" => s"(f_date_time < '${lastDateTime}' or f_date_time not like '%:00') and f_date_time != '${nodeTime}'" + keepDateTimeStr //回看类型保存当前和24小时历史整点
      case _ => "unknown"
    }
    if(countTableName == "unknown"){
      println("表名错误！统计类型" + nodeType)
    }else{
      val deleteSql =
        s"""
           |delete from $countTableName where ${deleteNodeStr}
        """.stripMargin
      try{
//        println("deleteSql:" + deleteSql)
        DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, deleteSql)
        println("成功删除历史数据!类型" + nodeType + ",时间：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM))
      }catch {
        case e:Exception => {
          println("删除历史数据失败!类型" + nodeType + ",时间：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM))
          e.printStackTrace()
        }
      }
    }
  }

  /**
    * 获取最近一个节点
    * @param sqlContext
    * @param nodeType
    * @return
    */
  def getLastNodeTime(sqlContext:HiveContext,nodeType:String) ={
    val lastNodeSql = s"(select max(f_date_time) as last_node_time from ${Tables.T_TIMENODE_REALTIME} where f_node_type='${nodeType}') as nd"
    val lastNodeTime =
      DBUtils.loadMysql(sqlContext,lastNodeSql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD).first().getAs[String]("last_node_time")
    if(lastNodeTime != null) lastNodeTime else "2019-03-01 00:00"
  }

  /**
    * 获取最近一个节点
    * @param nodeType
    * @return
    */
  def getLastNodeTime(nodeType:String) ={
    var con: java.sql.Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    var lastNodeTime = "2019-03-01 00:00"
    val lastNodeSql = s"select max(f_date_time) as last_node_time from ${Tables.T_TIMENODE_REALTIME} where f_node_type='${nodeType}'"
    try {
      con = DriverManager.getConnection(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
      pstmt = con.prepareStatement(lastNodeSql)
      rs = pstmt.executeQuery()
      rs.first()
      lastNodeTime = rs.getString(1)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if(rs != null) rs.close()
      if (pstmt != null) pstmt.close()
      if (con != null) con.close()
    }
    lastNodeTime
  }

  /**
    * 获取最近几个节点
    * @param nodeType
    * @param topNum
    */
  def getLastTopNodeTime(nodeType:String,topNum:Int) ={
    var con: java.sql.Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    var lastNodeTime = "2019-03-01 00:00"
    val topListBuffer = ListBuffer[String]()
    val lastNodeSql = s"select distinct f_date_time from ${Tables.T_TIMENODE_REALTIME} where f_node_type='${nodeType}' order by f_date_time desc limit ${topNum}"
    try {
      con = DriverManager.getConnection(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
      pstmt = con.prepareStatement(lastNodeSql)
      rs = pstmt.executeQuery()
      while(rs.next()){
        topListBuffer.append(rs.getString(1))
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if(rs != null) rs.close()
      if (pstmt != null) pstmt.close()
      if (con != null) con.close()
    }
    topListBuffer
  }
}
