package cn.ipanel.etl

import cn.ipanel.common.{DBProperties, SparkSession, Tables}
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}


/**
  *
  *数据初始化工具
  * 1.初始化省市县区域信息
  * 2.导出account_token数据到hive
  * Author :liujjy  
  * Date : 2019-03-20 0020 10:12
  * version: 1.0
  * Describle: 
  **/
object InitTools {

  def main(args: Array[String]): Unit = {

    val session = new SparkSession("InitTools")
    val sqlContext = session.sqlContext
    initArear(sqlContext)
    exportHistoryToken(sqlContext)


  }

  private def exportHistoryToken(sqlContext: HiveContext) {
    sqlContext.sql("use bigdata")
    val sql =
      """
        |(select ID,DA,device_id,device_type,access_token,expires_in,last_refresh_time,
        |user_type,channel_group_id,thirdparty_token,f_create_time,f_status,f_extend
        |from account_token) t
      """.stripMargin

    val tokenDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val sql2 =
      """
        |(select ID,DA,device_id,device_type,access_token,expires_in,last_refresh_time,
        |user_type,channel_group_id,thirdparty_token,f_create_time,f_status,f_extend
        |from t_account_token_history_201803) t
      """.stripMargin
    val tokenDF2 = DBUtils.loadMysql(sqlContext, sql2, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    tokenDF.unionAll(tokenDF2).dropDuplicates(Seq("DA"))
//    .repartition(20)
    .write.format("orc").mode(SaveMode.Overwrite).insertInto(Tables.ACCOUNT_TOKEN)
//    tokenDF.show(10)
    tokenDF.printSchema()
  }


  private def initArear(sqlContext: SQLContext) {
    val proviceSql = " (select  province_id,province_name from province ) t1"
    val citySql = " (SELECT province_id,city_id,city_name from city  ) t2"
    val areaSql = " (SELECT  city_id, area_id,area_name from area ) t3"

    val proviceDF = DBUtils.loadMysql(sqlContext, proviceSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val cityDF = DBUtils.loadMysql(sqlContext, citySql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val areaDF = DBUtils.loadMysql(sqlContext, areaSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    sqlContext.sql("use bigdata")
    proviceDF.join(cityDF, Seq("province_id")).join(areaDF, Seq("city_id"))
      .selectExpr("province_id", "province_name", "city_id", "city_name", "area_id", "area_name")
      .repartition(1).write.format("orc").mode(SaveMode.Overwrite).insertInto(Tables.ORC_AREA)

  }
}
