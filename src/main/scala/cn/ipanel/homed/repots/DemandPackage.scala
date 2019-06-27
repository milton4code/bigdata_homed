package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, SparkSession, UserActionType}
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import cn.ipanel.homed.repots.MultiScreen.writeToHive

import scala.collection.mutable.ArrayBuffer

/**
  * 点播套餐
  */
object DemandPackage {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |请输入参数 日期,分区数
        """.stripMargin)
      sys.exit(-1)
    }
    val date = args(0) //yyyy-MM-dd
    val partnum = args(1).toInt
    val nowdate = DateUtils.getAfterDay(date)
    val sparkSession = SparkSession("DemandPackage")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    //val querySql = s"( select * from table  where invalid = 0 and exp_date >'$date') t"
    //val user_pay_infoDF = MultilistUtils.getMultilistData("homed_iusm", "user_pay_info", sqlContext, querySql)
    //user_pay_infoDF.registerTempTable("user_pay_info")
  //  user_pay_infoDF.count()
    //产品包 栏目 单集id—套餐
    val programPackage = getProgramPackage(sqlContext)
    //剧集id-产品包
    val groupProgram = getGroupProgram(sqlContext)
    //用户观看记录
    val userInfo = getBasic(sqlContext, date)
    //用户观看剧集——产品包——套餐
    val basicdf = userInfo.join(groupProgram, Seq("f_series_id"))
    //产品包和套餐关联
    val groupPackage = basicdf.join(programPackage, programPackage("groupPackageID") === basicdf("f_group_id"))
      .select("package_id", "package_name", "f_cp_sp", "groupPackageID", "f_series_id", "f_terminal", "f_region_id", "f_region_name", "f_city_id", "f_city_name"
        , "f_province_id", "f_province_name", "f_user_id", "f_video_id", "f_video_name",
        "f_series_name", "f_play_time", "f_count", "f_device_id")
    //栏目和套餐关联
    //剧集+栏目+tree_id
    val seriesCloumn = getSeriesColumn(sqlContext)
    //tree_id+平台
    val treeColumn = gettreeColumn(sqlContext)
    //剧集+栏目+平台
    val finalColumndf = treeColumn.join(seriesCloumn, Seq("f_tree_id"))
    //和日志关联 节目—剧集+平台—栏目
    val basicColumn = userInfo.join(finalColumndf, Seq("f_series_id", "f_terminal"))
    val columnPackage = programPackage.join(basicColumn, programPackage("groupPackageID") === basicColumn("f_column_id"))
      .drop("f_group_id").drop("f_column_id").drop("f_tree_id")
    //套餐基础数据
    val userPackagefinal = groupPackage.unionAll(columnPackage)
    //用户——有效套餐
    //val userPackage = getUserPackage(sqlContext, date)
    //用户所属套餐 去除用户没有有效的套餐
    //val userPackagefinal = userPackage.join(finalInfoPackage, Seq("f_user_id", "package_id"))
    //套餐基础数据
    getBasicUserDemand(userPackagefinal, sqlContext, date, partnum)
    println("套餐基础数据统计结束")
    getUserWatchPackage(date: String, sqlContext: HiveContext)
    println("套餐观看统计结束")
    getUserCountPackage(date: String, sqlContext: HiveContext)
    println("套餐用户统计结束")
   // user_pay_infoDF.unpersist()
    sc.stop()
  }

  def gettreeColumn(sqlContext: HiveContext) = {
    val sql =
      """
        |(SELECT f_tree_id,f_device_type as f_terminal
        |FROM	`t_column_device`
        | ) as aa
      """.stripMargin
    val treecolumnDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    treecolumnDF
  }

  def getSeriesColumn(sqlContext: HiveContext) = {
    val sql =
      s"""
         |(SELECT distinct(f_column_id),f_real_program_id as f_series_id,f_tree_id
         |FROM	`t_column_program` WHERE f_program_status = 1 and f_is_hide = 0
         |and f_real_program_id between 300000000 and 399999999
         |) as aa
      """.stripMargin
    val columnDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    columnDF
  }


  def getUserCountPackage(date: String, sqlContext: HiveContext) = {

    val df = sqlContext.sql(
      s"""
           select '$date' as f_date,
         |a.f_terminal,a.f_region_id,a.f_region_name,a.f_province_id,a.f_province_name,
         |a.f_city_id,a.f_city_name,a.f_cp_sp,a.f_package_id,a.f_package_name,
         |a.f_user_type,count(*) as f_user_count
         |from (
         |select
         |f_terminal,f_region_id,f_region_name,f_province_id,f_province_name,
         |f_city_id,f_city_name,f_cp_sp,
         |f_package_id,f_package_name,
         |f_user_id,
         |sum(f_play_time) as f_play_time,
         |(case  when  sum(f_play_time)<=600 then 1
         |when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2
         |else 3 end) as f_user_type
         |from orc_user_package where day = '$date'
         |group by f_terminal,f_region_id,f_province_id,f_province_name,
         |f_city_id , f_city_name, f_region_id,f_region_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_user_id) a
         | group by
         | a.f_terminal,a.f_region_id,a.f_region_name,a.f_province_id,a.f_province_name,
         |a.f_city_id,a.f_city_name,a.f_package_id,a.f_package_name,a.f_cp_sp,
         |a.f_user_type
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, cn.ipanel.common.Tables.t_package_user_count)
  }

  //  def getBasicUserDemand(userPackage: DataFrame, userPackagefinal: DataFrame, sqlContext: HiveContext, date: String, partnum: Int) = {
  //    userPackage.repartition(partnum).registerTempTable("userPackage")
  //    userPackagefinal.repartition(partnum).registerTempTable("userPackagefinal")
  //    val sql1 =
  //      s"""
  //         |insert overwrite table  orc_user_package  partition(day='$date')
  //         |select
  //         |b.f_terminal,b.f_region_id,b.f_region_name,b.f_province_id,b.f_province_name,
  //         |b.f_city_id,b.f_city_name,b.f_cp_sp,
  //         |b.package_id as f_package_id,b.package_name as f_package_name,
  //         |b.f_user_id,
  //         |b.f_play_time,b.f_count
  //         |from userPackage a
  //         |join userPackagefinal b
  //         |on a.f_user_id=b.f_user_id and a.package_id=b.package_id
  //          """.stripMargin
  //    // writeToHive(sql1: String, sqlContext: SQLContext, cn.ipanel.common.Tables.orc_user_package, date)
  //  }
  def getBasicUserDemand(userPackagefinal: DataFrame, sqlContext: HiveContext, date: String, partnum: Int) = {
    userPackagefinal.repartition(partnum).registerTempTable("userPackagefinal")
    val sql =
      s"""
         |insert overwrite table  userprofile.orc_user_package  partition(day='$date')
         |select
         |b.f_terminal,b.f_region_id,b.f_region_name,b.f_province_id,b.f_province_name,
         |b.f_city_id,b.f_city_name,b.f_cp_sp,
         |b.package_id as f_package_id,b.package_name as f_package_name,
         |b.f_user_id,b.f_series_id,b.f_series_name,b.f_video_id,
         |b.f_video_name,
         |sum(b.f_play_time) as f_play_time,count(*) as f_play_count,
         |b.f_device_id
         |from userPackagefinal b
         |group by
         |b.f_terminal,b.f_region_id,b.f_region_name,b.f_province_id,b.f_province_name,
         |b.f_city_id,b.f_city_name,b.f_cp_sp,b.package_id,
         |b.package_name,b.f_user_id,
         |b.f_series_id,b.f_series_name,b.f_video_id,
         |b.f_video_name,b.f_device_id
      """.stripMargin
    writeToHive(sql, sqlContext: SQLContext, cn.ipanel.common.Tables.orc_user_package, date)

  }

  def getUserWatchPackage(date: String, sqlContext: HiveContext) = {
    sqlContext.sql("use userprofile")
    val df = sqlContext.sql(
      s"""
         |select '$date' as f_date,f_terminal,
         |f_region_id,f_region_name,f_province_id,f_province_name,f_city_id,f_city_name,f_cp_sp,
         |f_package_id,f_package_name,sum(f_count) as f_count,
         |sum(f_play_time) as f_play_time
         |from orc_user_package where day='$date'
         |group by
         |f_terminal,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id ,f_package_name,f_cp_sp
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, cn.ipanel.common.Tables.t_package_demand_count)
  }

  def getUserPackage(sqlContext: HiveContext, date: String) = {
    val sql =
      s"""
         |select a.operator_id as f_user_id,a.buy_time,
         |if((a.charge_type=1),a.f_parent_charge_id, a.charge_id) as package_id
         |from user_pay_info a
      """.stripMargin
    //val df = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    sqlContext.sql(sql)
  }

  def getBasic(sqlContext: HiveContext, date: String) = {
    sqlContext.sql("use userprofile")
    sqlContext.sql(
      s"""

         |select f_terminal,f_region_id,f_region_name,f_city_id,f_city_name,
         |f_province_id,f_province_name,f_user_id,f_video_id,f_video_name,
         |f_series_id,f_series_name,sum(f_play_time) as f_play_time,
         |count(*) as f_count,f_device_id
         |from
         |t_demand_video_basic
         |where day ='$date'
         |group by
         |f_terminal,f_region_id,f_region_name,f_city_id,f_city_name,
         |f_province_id,f_province_name,f_user_id,
         |f_series_id,f_series_name,f_video_id,f_video_name,f_device_id
      """.stripMargin)
  }

  def getGroupProgram(sqlContext: HiveContext) = {
    val sql =
      """(
            select f_group_id,f_program_id as f_series_id from t_group_program
            ) as vod_info
      """.stripMargin
    val groupInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    groupInfoDF
  }

  def getProgramPackage(sqlContext: HiveContext) = {
    import sqlContext.implicits._
    val sql =
      s"""(
            select package_id,package_name,
            if((f_service_provider is null or f_service_provider=''),f_content_provider,f_service_provider) as f_cp_sp,
            CONVERT(programIDs USING utf8) as programID from program_package
            where  status=${UserActionType.BUSINESS_PACKAGE_EFFECTIVE.toInt}
            ) as vod_info
      """.stripMargin
    val packageInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    //    1.regexp_extract('xxx','^\\[(.+)\\]$',1) 这里是把需要解析的json数组去除左右中括号，需要注意的是这里的中括号需要两个转义字符\\[。
    //    2.regexp_replace('xxx','\\}\\,\\{', '\\}\\|\\|\\{') 把json数组的逗号分隔符变成两根竖线||，可以自定义分隔符只要不在json数组项出现就可以。
    //    3.使用split函数返回的数组，分隔符为上面定义好的。
    //    4.lateral view explode处理3中返回的数组。
    //    import sqlContext.implicits._
    //    packageInfoDF.map(x => {
    //      val package_name = x.getAs[String]("package_name")
    //      val id = x.getAs[String]("programID").replaceAll("(\0|\\s*|\r|\n|\t)", "")
    //        .replace("[", "")
    //        .replace("]", "")
    //        .replace("},{", "}||{")
    //      (package_name,id)
    //    }).toDF("package_name", "id").registerTempTable("packageInfoDF")

    packageInfoDF.map(x => {
      val package_id = x.getAs[Long]("package_id")
      val package_name = x.getAs[String]("package_name")
      val f_cp_s = x.getAs[String]("f_cp_sp")
      val id = x.getAs[String]("programID").replaceAll("(\0|\\s*|\r|\n|\t)", "")
        .replace("[", "")
        .replace("]", "")
        .replace("},{", "}||{")
        .replace(",\"", "\",\"")
        .replace("\":", "\":\"")
        .replace("}", "\"}")
      (package_id, package_name, id, f_cp_s)
    }).toDF("package_id", "package_name", "id", "f_cp_sp")
      .registerTempTable("packageInfoDF")
    sqlContext.sql(
      """
        |select pp.package_id,pp.package_name,pp.f_cp_sp,get_json_object(ss.col,'$.ids') as groupPackageID,
        | get_json_object(ss.col,'$.type') as f_type
        |from(
        |select package_id,package_name,f_cp_sp,split(id,'\\|\\|') as str
        |from packageInfoDF )pp
        |lateral view explode(pp.str) ss as col
      """.stripMargin).registerTempTable("packageInfoDF1")
    sqlContext.sql(
      """
        |select pp.package_id,pp.package_name,pp.f_cp_sp,ss.col as groupPackageID
        |from(
        |select package_id,package_name,f_cp_sp,split(groupPackageID,'\\,') as str
        |from packageInfoDF1)pp
        |lateral view explode(pp.str) ss as col
      """.stripMargin)
  }

  def getIDS(program: String, keyWord: String) = {
    var programId = ""
    programId = getKeywords(program, keyWord)
    programId
  }

  /**
    * 根据正则抽取关键字段
    *
    * @param line 不允许为空
    * @return
    */
  def getKeywords(line: String, keyword: String): String = {
    val listBuffer = new ArrayBuffer[(String)]()
    var result = ""
    val fields = line.replaceAll("(\0|\\s*|\r|\n|\t)", "")
      .replace("[", "")
      .replace(":", "")
      .replace("{", "")
      .replace(" ", "").split("\\]\\,|\\}\\,")
    for (x <- fields) {
      if (x.contains(keyword)) {
        result = x.replace(keyword, "").trim.replace("}", "")
        listBuffer += (result)
      }
    }
    listBuffer.toString()
  }
}
