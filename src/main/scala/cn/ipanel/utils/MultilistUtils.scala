package cn.ipanel.utils

import cn.ipanel.common.{Constant, SparkSession}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.xml.XML

/**
  *
  *多表数据工具类
  * Author :liujjy  
  * Date : 2019-03-17 0017 21:51
  * version: 1.0
  * Describle: 
  **/
object MultilistUtils {

  /**
    * 获取分表的数据
    * @param table_schema 数据库名
    * @param query_table  表名
    * @param sqlContext
    * @param querySql 时间字段(做过滤用)
    * @return
    */
  def getMultilistData(table_schema:String, query_table:String, sqlContext: SQLContext,querySql:String="table")={
     val homedVersion = getHomedVersion()
     if( homedVersion != 0.0F && Constant.OLDER_HOMED_OVERION  >= homedVersion ) {
       val queryTable =  querySql.replace("table",query_table)
       get(table_schema,sqlContext,queryTable)
     }else{
       val tablesSql =
         s"""
            |(select table_name  from information_schema.tables
            | where table_schema='$table_schema' and table_name REGEXP '${query_table}_[0-9]{1,3}' )t
       """.stripMargin
       val df = get(table_schema,sqlContext,tablesSql).collect()
       val allDataDF = df.map(x=>{
         val table = x.getString(0)
         val queryTable =  querySql.replace("table",table)
         get(table_schema,sqlContext,queryTable)
       }).reduce((x1,x2)=> x1.unionAll(x2))

       allDataDF.repartition(allDataDF.rdd.getNumPartitions/df.length)
     }

  }


  private def get(table_schema:String,sQLContext:SQLContext,sql:String):DataFrame={
    import cn.ipanel.common.DBProperties._
    val schma = table_schema.substring(table_schema.indexOf("_")+1).toLowerCase()

    val jdbc = s"jdbc_url_$schma".toUpperCase
    val user = s"user_$schma".toUpperCase
    val password = s"password_$schma".toUpperCase
    println("sql========" + sql)
    DBUtils.loadMysql(sQLContext,sql,dbMap.get(jdbc),dbMap.get(user),dbMap.get(password) )
  }

  def getHomedVersion():Float={
    var version = 0.0f
    val xml = XML.loadFile("/homed/config_comm.xml")
//    val xml = XML.loadFile("D:\\idea_project\\homed\\bigdata_homed\\src\\test\\scala\\test\\demo.xml")
   try{
     val x = (xml \"project_info"\"version").text.trim.substring(1)
     version = x.substring(0,x.lastIndexOf(".")).toFloat
   }catch {
     case ex : Exception=>  {
       val x = (xml \"homed_version"\"version").text.trim.substring(1)
       version = x.substring(0,x.lastIndexOf(".")).toFloat
     }
   }
    println("version--------->>>>" + version)
    version
  }


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession("TotalUserStatistics")
    val sqlContext = sparkSession.sqlContext
    val date = "20190401"
    val asDateTime = DateUtils.dateStrToDateTime(date,DateUtils.YYYYMMDD).plusDays(1).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val querySql = s"( select * from table where f_create_time<'$asDateTime') t"
    val tokenDF = MultilistUtils.getMultilistData("homed_iusm","account_token",sqlContext,querySql)

    tokenDF.show()
  }
}
