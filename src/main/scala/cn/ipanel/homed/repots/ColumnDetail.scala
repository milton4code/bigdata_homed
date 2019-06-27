package cn.ipanel.homed.repots
import java.sql.DriverManager
import java.util

import cn.ipanel.common.{SparkSession, _}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
/**
  * 栏目点击次数报表统计
  * @author Liu gaohui
  * @date 2018/01/18
  * */
/**  phoenix建表语句
drop table t_user_column;
create table if not exists t_user_column(
f_date VARCHAR not null,
f_userid  INTEGER not null,
f_hour VARCHAR not null,
f_timerange VARCHAR not null,
f_column_id INTEGER not null,
f_pv INTEGER,
f_terminal VARCHAR not null,
f_column_name VARCHAR,
f_parent_id INTEGER,
f_region_id INTEGER not null,
f_region_name VARCHAR,
f_city_id INTEGER,
f_city_name VARCHAR,
f_province_id INTEGER,
f_province_name VARCHAR
CONSTRAINT PK PRIMARY KEY(f_date,f_userid,f_hour,f_timerange,f_column_id,f_terminal,f_region_id)
);
  */
@Deprecated
object ColumnDetail {
  def main(args: Array[String]): Unit = {
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }

    val sparkSession = SparkSession("ColumnDetail")
    val hiveContext = sparkSession.sqlContext

    val columnInfoMap = toParent(hiveContext)
    import sparkSession.sqlContext.implicits._

    //通过hiveContext读取json日志，分析log，提取出accesstoken和栏目id
    hiveContext.sql(s"use ${Constant.HIVE_DB}")
    val lines_base = hiveContext.sql(
      s"""
         |select report_time,params['accesstoken'] as accesstoken,
         |params['label'] as label
         |from orc_nginx_log
         |where key_word like '%/homed/program/get_list%' and day='$day'
            """.stripMargin)
    val lines = lines_base.map(x=>{
      val report_time = x.getAs[String]("report_time") //2018-05-04 08:51:26
      val date = report_time.split(" ")(0).replace("-","")
      val hour =  report_time.split(" ")(1).split(":")(0)
      val minute = report_time.split(" ")(1).split(":")(1)
      val f_timerange = getTimeRangeByMinute(minute.toInt)
      val accesstoken = x.getAs[String]("accesstoken")
      val keyarr = TokenParser.parserAsUser(accesstoken)//val arr = Token.parser(accesstoken)
      val f_userid = keyarr.DA.toString
      val deviceId = keyarr.device_id
      val f_terminal = keyarr.device_type
      val f_province_id = keyarr.province_id
      val f_city_id = keyarr.city_id
      val f_region_id = keyarr.region_id
      val label = x.getAs[String]("label")
      val key = date + "," + hour + "," +f_timerange+","+ f_userid+","+f_terminal+","+f_province_id+","+f_city_id+","+f_region_id  + "," + label
      (key, 1)
      }).filter(x => {
      !"null".equals(x._1.split(",")(4))&& !"0".equals(x._1.split(",")(4))
    }).reduceByKey(_ + _).map(x => {
      val arr = x._1.split(",")
      val f_date = arr(0)
      val f_hour = arr(1)
      val f_timerange = arr(2)
      val f_userid = arr(3)
      val f_terminal = arr(4)
      val f_province_id = arr(5)
      val f_city_id = arr(6)
      val f_region_id = arr(7)
      val label = arr(8)
      val f_pv = x._2
      ColumnAnaly(f_date, f_hour,f_timerange,f_userid,f_terminal,f_province_id,f_city_id,f_region_id,label, f_pv)
    }).filter(x=>x.f_userid!="0").toDF()
    columnDetail(lines, sparkSession,columnInfoMap)

   sparkSession.stop()
  }

  /***
    * 读取dtvs数据库，通过栏目id找到parent_id,column_index,label_name字段信息
    * 读取iusm数据库，通过access_token去找DA,再通过DA去找到用户的基础信息
    * @param detailsDF
    * @param sparkSession
    * @param column
    */
  def columnDetail(detailsDF: DataFrame, sparkSession: SparkSession,column:util.HashMap[Long,String]): Unit = {
    val sqlContext = sparkSession.sqlContext
    import sparkSession.sqlContext.implicits._
    val columnsql = "( select distinct f_column_id ,f_parent_id, f_column_name  from t_column_info ) as column_info"
    val provincesql= "(SELECT province_id as f_province_id,province_name as f_province_name from province ) as aa"

    val citysql= s"(SELECT city_id as f_city_id,city_name as f_city_name from city ) as aa"
    val regionsql= "(SELECT area_id as f_region_id,area_name as f_region_name from area ) as aa "

    val provinceDF = DBUtils.loadMysql(sqlContext, provincesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val cityDF = DBUtils.loadMysql(sqlContext, citysql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val regionDF = DBUtils.loadMysql(sqlContext, regionsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val columnInfoDF = DBUtils.loadMysql(sqlContext, columnsql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val result = detailsDF.join(columnInfoDF,detailsDF("label")===columnInfoDF("f_column_id"))
      .join(provinceDF,Seq("f_province_id"))
      .join(cityDF,Seq("f_city_id"))
      .join(regionDF,Seq("f_region_id"))
      .drop("label")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    result.registerTempTable("result")
    DBUtils.saveToHomedData_2(result, Tables.mysql_user_column)
//          DBUtils.saveDataFrameToPhoenixNew(column_base, Tables.hbase_user_column)
    //读取出栏目的游览的详细信息，包括游览的用户及游览次数
    val column_count = sqlContext.sql(
      """
        |select sum(f_pv) as f_pv,
        |""  as f_userid_pv,
        |f_date,f_hour,f_timerange,f_column_id,
        |f_region_id,f_region_name,f_province_id,f_province_name,f_city_id,f_city_name,f_terminal
        |from result
        | group by f_date,f_hour,f_timerange,f_column_id,
        |f_region_id,f_region_name,f_province_id,f_province_name,f_city_id,f_city_name,f_terminal
      """.stripMargin)
    // print("================column_count=========")
    val column_info = result.map(x=>{
      val column_id = x.getAs[Long]("f_column_id")
      val column_info = getColumnRoot(column_id,column)  //将栏目信息构成listBuffer
      var f_column_id:Long=0L                            //解析listBuffer  将栏目id,栏目名字及父栏目信息读出
      var f_column_name:String=""
      var f_parent_column_id:Long=0L
      var f_parent_column_name:String=""
      var f_parent_parent_column_id:Long=0L
      var f_parent_parent_column_name:String=""
      val f_column_level=column_info.length-1           //得出栏目的层级
      var j = 0
      for(tuple <- column_info){
        if(j==0){
          f_column_id=tuple._1
          f_column_name=tuple._2
        }else if(j==1){
          f_parent_column_id=tuple._1
          f_parent_column_name=tuple._2
        }else if(j==2){
          f_parent_parent_column_id=tuple._1
          f_parent_parent_column_name=tuple._2
        }
        j+=1
      }
      Column_info(f_column_level, f_column_id, f_column_name,f_parent_column_id,f_parent_column_name,
        f_parent_parent_column_id,f_parent_parent_column_name)                 //返回栏目的层级信息
    }).toDF().distinct()

    result.unpersist()
    val  column_count_info = column_info.join(column_count,Seq("f_column_id"))
    DBUtils.saveToHomedData_2(column_count_info,Tables.T_USER_COLUMN_SUMMARY)

    sparkSession.stop()
  }



  /***
    * 将一个小时分成2块
    * 0-30分钟 为30
    * 30-60分钟为60
    * @param minute
    * @return
    */
  def getTimeRangeByMinute(minute: Int): String = {
    var timeRange = "00-30"
    minute match {
      case _ if (minute >= 30) => timeRange = "60"
      case _ => timeRange = "30"
    }
    timeRange
  }

  /***
    * 读取栏目信息表column_info
    * 将column_id  column_name和parent_id构成一个map结构
    * @param hiveContext
    * @return
    */
  def toParent(hiveContext: HiveContext): util.HashMap[Long,String] = {
    val map=new java.util.HashMap[Long,String]()
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS,DBProperties.USER_DTVS,DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(
      "SELECT f_column_id,f_parent_id,f_column_name FROM	`t_column_info` WHERE f_column_status = 1")
    while (queryRet.next) {
      val f_column_id=queryRet.getLong("f_column_id")
      val f_column_name=queryRet.getString("f_column_name")
      val f_parent_id=queryRet.getLong("f_parent_id")
      map.put( f_column_id,f_parent_id + "," + f_column_name)
    }
    connection.close
    map
  }

  /***
    * 根据一个column_id确定是第几级栏目，找到该栏目的父栏目信息
    * @param colId
    * @param columnInfoMap
    * @return
    */
  def getColumnRoot(colId: Long,columnInfoMap:util.HashMap[Long,String]) = {
    var mapId = colId
    var find = false
    val listBuffer = ListBuffer[(Long,String)]()
    val tmplist = Constant.COLUMN_ROOT
    while (!find&&columnInfoMap.keySet().contains(mapId)) {
      val keyInfo = columnInfoMap.get(mapId).split(",")
      val keyId = keyInfo(0)
      var keyName = keyInfo(1)
      if (tmplist.contains(keyId)) {
        find = true
        keyName = keyInfo(1)
      }
      listBuffer += ((mapId,keyName))
      mapId =columnInfoMap.get(mapId).split(",")(0).toLong
    }
    listBuffer
  }
}
