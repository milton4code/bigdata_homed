package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, SparkSession, Tables}
import cn.ipanel.utils._
/**
  * 个人开户报表统计
  *
  * @author ZouBo
  * @date 2018/1/10 0010 
  */
object PersonalOpenAccount {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("参数错误,请输入日期 例如 [20181010]")

      sys.exit(-1)
    } else {
      val day = args(0)
      val nextDay = DateUtils.getAfterDay(day,targetFormat = DateUtils.YYYY_MM_DD)
      val sparkSession = SparkSession("PersonalOpenAccount")
      val hiveContext = sparkSession.sqlContext

      var regionCode = RegionUtils.getDefaultRegion()

      val startDay = DateUtils.dateStrToDateTime(day,DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
      val querySql = s"( select * from table where f_create_time>'$startDay' and f_create_time<'$nextDay') t"
      val account_tokenDF = MultilistUtils.getMultilistData("homed_iusm","account_token",hiveContext,querySql)
      account_tokenDF.registerTempTable("account_token")

      val queryinfojoinboss =
      s"""
         |(SELECT
         |	a.DA AS f_da,a.account_name AS f_account_name,a.sex AS f_sex,
         |	a.nick_name AS f_nickname,a.create_time AS f_open_account_time,a.home_id AS f_home_id,
         |  REPLACE(REPLACE(a.group_ids ,'[',''),']','') as f_group_ids,
         |  a.f_reg_source AS  f_user_source,
         |  if(length(cast(t.f_user_area_id as CHAR))=6,t.f_user_area_id,${regionCode}) as f_user_area_id,
         | t.f_source ,t.f_customer_code AS f_customer_code
         |FROM
         |	account_info a
         |LEFT JOIN t_da_boss_info t ON a.DA = t.f_da
         |WHERE
         |	a.create_time BETWEEN '$startDay' AND '$nextDay'
         |  ) as info_boss
          """.stripMargin

      //设备信息，一个homeId对应多个deviceID,一个deviceID对应多个终端
      val querydeviceSql =
      """
        |(SELECT
        |	home_id as device_home_id,
        | GROUP_CONCAT(CONCAT(device_id,'(',CONCAT_WS(',',if(LENGTH(cai_id)>0,cai_id,null),
        | if(LENGTH(mobile_id)>0,mobile_id,null),if(LENGTH(pad_id)>0,pad_id,null),
        | if(LENGTH(stb_id)>0,stb_id,null))),')') as f_device_series_ids
        |	FROM
        |		device_info
        |	GROUP BY
        |		home_id) as device
      """.stripMargin

      val querytokenSql =
        s"""
           |select first(DA) as token_da,last(f_extend) as f_app_version,MIN(b.f_create_time) as f_first_login_time
           |from account_token b
           |group by b.DA
        """.stripMargin


      val tokenDf = hiveContext.sql(querytokenSql)

      tokenDf.registerTempTable("token_info")
      val tokenSql=
        """
          |select token_da,first(f_app_version) as f_app_version,min(f_first_login_time) as f_first_login_time
          |from token_info
          |group by token_da
        """.stripMargin

      val tokenResult=hiveContext.sql(tokenSql)
      val infoBossDf = DBUtils.loadMysql(hiveContext, queryinfojoinboss, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM,
        DBProperties.PASSWORD_IUSM)
      val deviceDf = DBUtils.loadMysql(hiveContext, querydeviceSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM,
        DBProperties.PASSWORD_IUSM)

      val regionDF = RegionUtils.getRegions(hiveContext)
                    .selectExpr("province_id f_province_id","province_name f_province_name",
                      "city_id f_city_id","city_name f_city_name","region_id f_area_id","region_name f_region_name")

      val joinDeviceDF = infoBossDf.join(deviceDf, infoBossDf("f_home_id") === deviceDf("device_home_id"), "left_outer")
        .drop("device_home_id")
      val joinTokenDF = joinDeviceDF.join(tokenResult, joinDeviceDF("f_da") === tokenDf("token_da"), "left_outer")
        .drop("token_da")


      val userDF = UserUtils.getHomedUserDevice(hiveContext, nextDay)
        .filter(s" create_time >='$startDay' and create_time < '$nextDay' ")
        .selectExpr("user_id as f_da")

      joinTokenDF.join(userDF, Seq("f_da")).registerTempTable("open_account_info")

      //用户来源9:游客用户 其他属注册用户
      //1,2,3,10,11归类为有限电视用户，4,5,6,7,8归类为ott，9是其他

      val resultSql =
        """
          |select f_da,f_account_name,f_sex,f_nickname,f_open_account_time,f_group_ids,
          |(case when  f_user_source in(1,2,3,10,11) then 1
          |when f_user_source in(4,5,6,7,8) then 2
          |else 3 end) as f_register_type,
          |f_user_area_id as f_region_id,
          |(case when f_user_source=9 then 2 else 1 end) as f_user_type,
          |f_user_source,f_device_series_ids,
          |f_customer_code,f_first_login_time,get_json_object(f_app_version,'$.appversion') as f_app_version
          |from open_account_info
        """.stripMargin
      val result = hiveContext.sql(resultSql)

      val df = result.join(regionDF, result("f_region_id") === regionDF("f_area_id")).drop("f_area_id")

      delMysql(day, Tables.t_personal_open_account)
      DBUtils.saveToHomedData_2(df, Tables.t_personal_open_account)

    }

  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(day: String, table: String): Unit = {
    val date = DateUtils.transformDateStr(day)
    val del_sql = s"delete from $table where f_open_account_time like '$date%'"
    println(s"delte........$del_sql" )
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

}
