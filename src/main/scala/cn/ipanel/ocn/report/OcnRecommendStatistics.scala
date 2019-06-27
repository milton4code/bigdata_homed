package cn.ipanel.ocn.report

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.ocn.etl.OcnConstant
import cn.ipanel.utils.DBUtils
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ListBuffer

/**
  * 东方有线推荐曝光/推荐成功uv和pv统计
  *
  * @author ZouBo
  * @date 2018/5/28 0028 
  */
case class Recommend(report_time: String, date: String, user_id: String, device_type: String, action: String, portalId: String, sceneId: String,
                     sceneName: String, recommendIdx: String, programId: String)

object OcnRecommendStatistics {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("参数错误，格式[yyyyMMdd]")
      System.exit(1)
    }

    val day = args(0)
    val sparkSession = SparkSession("OcnRecommendStatistics", "")
    val hiveContext = sparkSession.sqlContext
    val sparkContext = sparkSession.sparkContext
    import hiveContext.implicits._
    sparkContext.getConf.registerKryoClasses(Array(classOf[Recommend]))
    hiveContext.sql("use bigdata")

    val sql =
      s"""
         |select report_time,'$day' as date,user_id,device_type,
         |params['A'] as action,params['P'] as portalId,params['SI'] as sceneId,
         |params['S'] as sceneName,params['RI']  as recommendIdx,
         |if((params['PI'] is null),'',params['PI']) as programId
         |from orc_ocn_reported
         |where day='$day' and service_type=${OcnConstant.RECOMMEND}

      """.stripMargin
    val df = hiveContext.sql(sql).mapPartitions(it => {
      val list = new ListBuffer[Recommend]
      it.foreach(row => {
        val time = row.getAs[String]("report_time")
        val date = row.getAs[String]("date")
        val userId = row.getAs[String]("user_id")
        val deviceType = row.getAs[String]("device_type")
        val action = row.getAs[String]("action")
        val portalId = row.getAs[String]("portalId")
        val sceneId = row.getAs[String]("sceneId")
        val sceneName = row.getAs[String]("sceneName")
        val recommendIdx = row.getAs[String]("recommendIdx")
        val programId = row.getAs[String]("programId")
        val recommendIdxArr = recommendIdx.split("\\|")
        for (recommendIndex <- recommendIdxArr) {
          list += (Recommend(time, date, userId, deviceType, action, portalId, sceneId, sceneName, recommendIndex, programId))
        }
      })
      list.iterator
    }).toDF
    val result = process(sparkSession, df)
    delMysql(day, "t_ocn_recommend_statistics")
    DBUtils.saveToMysql(result, "t_ocn_recommend_statistics", DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD)
  }

  def process(sparkSession: SparkSession, df: DataFrame) = {
    val sqlContext = sparkSession.sqlContext
    df.registerTempTable("t_recommend")
    //1.统计曝光pv/uv
    val exposureSql =
      """
        |select date as f_date,device_type as f_terminal,portalId as f_portal_id,
        |sceneId as f_scene_id ,sceneName as f_scene_name,
        |recommendIdx as f_recommend_index,
        |count(1) as f_recommend_pv,
        |count(distinct user_id) as f_recommend_uv
        |from t_recommend
        |where action='1'
        |group by date,device_type,portalId,sceneId,sceneName,recommendIdx
      """.stripMargin
    val exposureDf = sqlContext.sql(exposureSql)
    //2.统计点击pv/uv
    val successSql =
      """
        |select date ,device_type ,portalId ,sceneId,sceneName ,recommendIdx ,
        |count(1) as f_click_pv,
        |count(distinct user_id) as f_click_uv
        |from t_recommend
        |where action='2'
        |group by date,device_type,portalId,sceneId,sceneName,recommendIdx
      """.stripMargin
    val successDf = sqlContext.sql(successSql)
    val result = exposureDf.join(successDf, exposureDf("f_date") === successDf("date") && exposureDf("f_terminal") === successDf("device_type")
      && exposureDf("f_portal_id") === successDf("portalId") && exposureDf("f_scene_id") === successDf("sceneId") && exposureDf("f_scene_name") === successDf("sceneName")
      && exposureDf("f_recommend_index") === successDf("recommendIdx"), "left_outer")
      .selectExpr("f_date", "f_portal_id", "f_scene_name", "f_recommend_index", "f_terminal", "f_recommend_pv",
        "f_recommend_uv", "f_click_pv", "f_click_uv", "f_scene_id").registerTempTable("t_recommend_2")
    val sql =
      """
        | select f_date,f_portal_id,f_scene_name,f_recommend_index,f_terminal,
        | if((f_recommend_pv is null),0,f_recommend_pv) as f_recommend_pv,
        | if((f_recommend_uv is null),0,f_recommend_uv) as f_recommend_uv,
        | if((f_click_pv is null),0,f_click_pv) as f_click_pv,
        | if((f_click_uv is null),0,f_click_uv ) as f_click_uv,
        | f_scene_id
        | from t_recommend_2
      """.stripMargin
    val sceneDF = getSceneData(sqlContext)
    sqlContext.sql(sql).join(sceneDF, Seq("f_scene_id"), "left").na.fill("")
  }

  //加载后台场景id 和场景名称
  def getSceneData(sqlContext: SQLContext): DataFrame = {

    val sql = """(select cast(record_value as char) as record_value from system_setting where record_name like 'scene%' and record_name like '%tmp%') as a"""
    val sceneDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    import sqlContext.implicits._

    val finaldf = sceneDF.map(x => {
      val value = x.getAs[String]("record_value")
      val json = JSON.parseObject(value)
      var scene_name = ""
      var scene_id = ""
      if (json.containsKey("tmp_base")) {
        val data = json.get("tmp_base")
        val json1 = JSON.parseObject(data.toString)
        if (json1.containsKey("tmp_name") && json1.containsKey("tmp_id")) {
          scene_name = json1.get("tmp_name").toString
          scene_id = json1.get("tmp_id").toString
        }
      }
      (scene_id, scene_name)
    }).filter(x => x._1 != "").toDF("f_scene_id", "f_scene_name_new")
    finaldf

  }

  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD, del_sql)
  }
}
