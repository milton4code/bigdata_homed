package cn.ipanel.homed.repots

import java.sql.DriverManager
import java.util

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, IDRangeUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import IDRangeConstant._

import scala.collection.mutable.ListBuffer

case class shareInfo(f_programid:String,f_share_count:Int)
case  class columnDD(f_programid:Long,f_column_id:String)
case class photoAppInfo(f_date:String,f_programid:Long,f_program_type:String,f_count:Int)
case class Column_info2(f_date:String,f_column_level: Int,f_programid:Long,f_program_type:String,f_share_count:Int,f_count:Int, f_column_id: Long,
                       f_column_name: String, f_parent_column_id: Long, f_parent_column_name: String,
                       f_parent_parent_column_id: Long, f_parent_parent_column_name: String)
/**
  * 图文点击报表
  */
object HitAnalysis {
  def main(args: Array[String]): Unit = {

    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }

    val sparkSession = SparkSession("HitAnalysis")
    val hiveContext =sparkSession.sqlContext

    import hiveContext.implicits._
    val columnInfoMap = toParent(hiveContext)
    hiveContext.sql(s"use ${Constant.HIVE_DB}")
     val program_base =
      s"""
         |select day,exts['ProgramID'] as ProgramId,exts['DeviceType'] as deviceType
         |from orc_user_behavior
         |where reporttype='ProgramEnter' and day='$day'
            """.stripMargin
    val colunm_base =
      s"""
         |select serviceid,ext['column_id'] as column_id
         |from orc_video_play
         |where playtype='demand' and day='$day'
            """.stripMargin
    val share_base =
      s"""
         |select day,params['roomid'] as videoid
         |from orc_nginx_log
         |where key_word like '%share/index.html%' and day='$day'
            """.stripMargin
    val programRdd = hiveContext.sql(program_base).rdd
    val shareRdd = hiveContext.sql(share_base).rdd
    val columnRdd = hiveContext.sql(colunm_base).rdd
    /**
      * 计算图文的的点击量
      */
    val programDF = programRdd.map(x=>{
        val f_date = x.getAs[String]("day")
        val f_device_type = x.getAs[String]("deviceType")
        val f_programid = x.getAs[String]("ProgramId").toLong
        photoCount(f_date, f_device_type, f_programid)
      }).filter(x => IDRangeUtils.isPhotoOrApp(x.f_porgramid))
        .map(x=>{
          val key = x.f_date+","+x.f_porgramid
          (key,1)
        }).reduceByKey(_+_).map(x=>{
      val arr = x._1.split(",")
      val f_date = arr(0)
      val f_programid = arr(1).toLong
      val f_program_type = if (f_programid> PHOTO_START &&f_programid< PHOTO_EDN) "photo" else "app"
      val f_count = x._2
      photoAppInfo(f_date, f_programid, f_program_type,f_count)
    }).toDF()

    val columnDF = columnRdd.map(x=>{
        val f_programid = x.getAs[Long]("serviceid")
        val f_column_id = x.getAs[String]("column_id")
      columnDD(f_programid,f_column_id)
      }).toDF()
    val shareDF = shareRdd.map(x=>{
      val f_date = x.getAs[String]("day")
      val f_videoid = x.getAs[String]("videoid")
      (f_date+","+f_videoid,1)
    }).reduceByKey(_+_).map(x=>{
      val f_date = x._1.split(",")(0)
      val f_programid = x._1.split(",")(1)
      val f_share_count = x._2
      shareInfo(f_programid,f_share_count)
    }).toDF()
    val lines = programDF.join(columnDF,Seq("f_programid"),"left").join(shareDF,Seq("f_programid"),"left")
    HitPhoto(hiveContext,lines,columnInfoMap)

    sparkSession.stop()
  }
  def HitPhoto(hiveContext: HiveContext,logInfo:DataFrame,column:util.HashMap[Long,String]) ={
    import hiveContext.implicits._
    val appInfosql =
      """(select id as f_programid, chinese_name as f_program_name from app_store
        |) as app
      """.stripMargin
    val newsInfosql =
      """(select f_news_id as f_programid, f_title as f_program_name from t_news_info where f_status = 65002
        |) as news
      """.stripMargin
    val appInfoDF = DBUtils.loadMysql(hiveContext, appInfosql, DBProperties.JDBC_URL_ICORE, DBProperties.USER_ICORE, DBProperties.PASSWORD_ICORE)
    val newsInfoDF = DBUtils.loadMysql(hiveContext, newsInfosql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val appNewsDF = appInfoDF.unionAll(newsInfoDF)
    val column_info = logInfo.map(x=>{             //通过栏目id，找到上一级上两级栏目
      val column_id = x.getAs[Long]("f_column_id")
      val f_programid = x.getAs[Long]("f_programid")
      val f_date = x.getAs[String]("f_date")
      val f_program_type = x.getAs[String]("f_program_type")
      val f_share_count = x.getAs[Int]("f_share_count")
      val f_count = x.getAs[Int]("f_count")
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
      Column_info2(f_date,f_column_level,f_programid,f_program_type,f_share_count,f_count, f_column_id, f_column_name,f_parent_column_id,
        f_parent_column_name, f_parent_parent_column_id,f_parent_parent_column_name)                 //返回栏目的层级信息
    }).toDF().distinct()
    val result =column_info.join(appNewsDF,Seq("f_programid"))
    DBUtils.saveToHomedData_2(result,Tables.Dalian_User_Hit)
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
    //val tmplist = List("100","961","962","5034","12071",
    // "12549","12891", "37845","37922")
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
}

