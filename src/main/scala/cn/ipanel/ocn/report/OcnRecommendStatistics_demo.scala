package cn.ipanel.ocn.report

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.homed.repots.SearchDetailNew.{getDataFrameByNum, getNumByResultId}
import cn.ipanel.ocn.etl.{OcnConstant, OcnLogParser}
import cn.ipanel.ocn.etl.OcnLogParser.process
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * 上海ocn 推荐demo
  *
  * @author ZouBo
  * @date 2018/5/28 0028 
  */


object OcnRecommendStatistics_demo {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("参数错误，格式[yyyyMMdd]")
      System.exit(-1)
    }

    val day = args(0)
    val sparkSession = SparkSession("OcnRecommendStatistics_demo")
    val hiveContext = sparkSession.sqlContext
    val sparkContext = sparkSession.sparkContext
    sparkContext.getConf.registerKryoClasses(Array(classOf[Recommend]))
    //    OcnLogParser.process(sparkSession,day)

    hiveContext.sql("use bigdata")

    //    val sql =
    //      s"""
    //         |select report_time,'$day' as date,user_id,device_type,
    //         |params['A'] as action,params['P'] as portalId,params['SI'] as sceneId,
    //         |params['S'] as sceneName,params['RI']  as recommendIdx,
    //         |if((params['PI'] is null),'',params['PI']) as programId
    //         |from orc_ocn_reported
    //         |where day='$day' and service_type=${OcnConstant.RECOMMEND} and params['A']='2'
    //      """.stripMargin

    val sql =
      s"""
         |select reporttime as report_time,'$day' as date, userid  as user_id, devicetype as device_type,
         |exts['A'] as action,exts['P'] as portalId,exts['SI'] as sceneId,
         |exts['S'] as sceneName,exts['RI']  as recommendIdx,
         |if((exts['PI'] is null),'',exts['PI']) as programId
         |from orc_user_behavior
         |where day='$day' and reporttype='124' and exts['A']='2'
      """.stripMargin
    val df = hiveContext.sql(sql)
    val result = process(sparkSession, df)
    // program id 集
    val resultList = new mutable.HashSet[Int]()
    result.select("f_program_id").distinct().collect().map(x => {
      val result_id = x.getAs[String]("f_program_id").toLong
      val num = getNumByResultId(result_id)
      resultList.add(num)
    })
    val eventDF = getDataFrameByNum(resultList, hiveContext).selectExpr("result_id as f_program_id", "f_keyword as f_program_name")
    val finalDF = result.join(eventDF, "f_program_id")
    //    finalDF.printSchema()
    delMysql(day, "t_ocn_recommend_statistics_demo")
    DBUtils.saveToMysql(finalDF, "t_ocn_recommend_statistics_demo", DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD)
    //    DBUtils.saveToMysql(finalDF, "t_ocn_recommend_statistics_demo", DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
  }

  def process(sparkSession: SparkSession, df: DataFrame) = {
    val sqlContext = sparkSession.sqlContext
    df.registerTempTable("t_recommend")
    //2.统计点击pv/uv
    val successSql =
      """
        |select date as f_date ,device_type as f_terminal ,portalId as f_portal_id ,sceneId as f_scene_id,sceneName as f_scene_name ,recommendIdx as f_recommend_index,
        |programId as f_program_id,
        |count(1) as f_click_pv,
        |count(distinct user_id) as f_click_uv
        |from t_recommend
        |where action='2'
        |group by date,device_type,portalId,sceneId,sceneName,recommendIdx,programId
      """.stripMargin
    val successDf = sqlContext.sql(successSql)
    successDf
  }


  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD, del_sql)
    //    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  case class Recommend_demo(report_time: String, date: String, user_id: String, device_type: String, action: String, portalId: String, sceneId: String,
                            sceneName: String, recommendIdx: String, programId: String)

}
