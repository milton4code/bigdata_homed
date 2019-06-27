package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.homed.repots.ClickUpload.writeToHive
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils, RegionUtils}
import org.apache.spark.sql.hive.HiveContext

/**
  * 套餐营收
  */
object UserBusiness {
  def main(args: Array[String]): Unit = {
    //套餐维度
    val sparkSession = SparkSession("UserBusiness")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val time = args(0) //日期
    val month = time.substring(0, 6) //月
    val year = time.substring(0, 4) //年

    val dateTime = DateUtils.dateStrToDateTime(time,DateUtils.YYYYMMDD)
    val startDay = dateTime.toString(DateUtils.YYYYMMDD)
    val endDay = dateTime.plusDays(1).toString(DateUtils.YYYYMMDD)

    val querySql1 = s"( select * from table where invalid=0 and buy_time BETWEEN '$startDay' and '$endDay' ) t"
    val user_pay_infoDF = MultilistUtils.getMultilistData("homed_iusm", "user_pay_info", sqlContext,querySql1)
    user_pay_infoDF.registerTempTable("user_pay_info")
    user_pay_infoDF.count()

    val querySql2 = s"( select * from table where  f_order_time  BETWEEN '$startDay' and '$endDay' ) t"
    val t_business_recordDF = MultilistUtils.getMultilistData("homed_iusm", "t_business_record", sqlContext,querySql2)
    t_business_recordDF.registerTempTable("t_business_record")
    t_business_recordDF.count()

    val quarterDate = DateUtils.getFirstDateOfQuarter(time)
    //季度
    val quarter = quarterDate.substring(0, 6)
    //本周第一天
    val week = DateUtils.getFirstDateOfWeek(time)
    //前7天 30 天 365天
    val beforeWeek = DateUtils.getDateByDays(time, 6)
    val beforeMonth = DateUtils.getDateByDays(time, 29)
    val beforeYear = DateUtils.getDateByDays(time, 364)
    val nextDate = DateUtils.getAfterDay(time)
    val regionCode = RegionUtils.getRootRegion
    var REGION = ""
    if (regionCode.endsWith("0000")) {
      REGION = regionCode.substring(0, regionCode.length - 3) + "101" //默认市的默认区
    } else {
      REGION = regionCode.substring(0, regionCode.length - 1) + "1" //默认区
    }
    //跟套餐订购有关的
    getBasicBusiness(sqlContext, time, nextDate, REGION, regionCode, month, year)

    t_business_recordDF.unpersist()
    user_pay_infoDF.unpersist()
    println("套餐明细统计成功")
  }


  def getUnsubscribeHistory(sqlContext: HiveContext, starttime: String, endtime: String, tableName: String, f_type: Int) = {
    sqlContext.sql("use userprofile")
    val sql = sqlContext.sql(
      s"""
         |select  '$starttime' as f_start_date, '$endtime' as f_end_date,
         |f_region_id,f_region_name,
         |f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_package_type,f_user_type,count(*) as f_count,
         |count(distinct(f_user_id)) as f_user_count,
         |sum(price) as f_price,'$f_type' as f_type
         |from
         |t_business_basic_by_day
         |where day between '$starttime' and '$endtime'
         |group by
         |f_region_id,f_region_name,
         |f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_package_type,f_user_type
          """.stripMargin)
    DBUtils.saveToHomedData_2(sql, tableName)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delAllMysql(table: String): Unit = {
    val del_sql = s"delete from $table"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  def getUnsubscribe(sqlContext: HiveContext, starttime: String, endtime: String, tableName: String, column: String) = {
    sqlContext.sql("use userprofile")
    val sql = sqlContext.sql(
      s"""
         |select  '$starttime' as f_date,
         |f_region_id,f_region_name,
         |f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_package_type,f_user_type,count(*) as f_count,
         |count(distinct(f_user_id)) as f_user_count,
         |sum(price) as f_price
         |from
         |t_business_basic_by_day
         |where $column between '$starttime' and '$endtime'
         |group by
         |f_region_id,f_region_name,
         |f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_package_type,f_user_type
          """.stripMargin)
    DemandMeizi.delMysql(starttime, tableName)
    DBUtils.saveToHomedData_2(sql, tableName)
  }

  //  订购变化类型 1:新增订购 2:新增退订 3:新增到期 4:新增续订

  def getBasicBusiness(sqlContext: HiveContext, day: String, _nextDate: String, REGION: String, regionCode: String, month: String, year: String) = {
    val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val date = dateTime.toString(DateUtils.YYYY_MM_DD)
    val nextDate = dateTime.plusDays(1).toString(DateUtils.YYYY_MM_DD)
    val querypaySql =
      s"""
         |select u.f_order_id,
         |(case  when  (u.f_reorder=1) then '4'
         |when (u.exp_date between '$date' and '$nextDate' and u.f_reorder=0) then '3'
         |when (u.buy_time between '$date' and '$nextDate' and u.f_reorder=0) then '1'
         |else '4' end) as f_user_type,
         | if((u.buy_time between '$date' and '$nextDate' ),u.price,0)  as price,
         |  if (datediff(u.exp_date , u.effect_time) = 0,1,datediff(u.exp_date , u.effect_time)+1) as f_cycle
         | from user_pay_info u
         | where ( (u.exp_date between '$date' and '$nextDate')
         |  or (u.buy_time between '$date' and '$nextDate'))
         |  and u.f_order_id is not null
         |
            """.stripMargin
    //    val userDF = DBUtils.loadMysql(sqlContext, querypaySql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val userDF = sqlContext.sql(querypaySql)

    val sql1 =
      s"""
         |SELECT f_order_id,f_operator_id as f_user_id,
         |f_product_name as f_package_name,
         |case when  length(cast(f_area_id as string))>=6 and substr(cast(f_area_id as string),5,2)!='00' then substr(f_area_id,1,6)
         | when length(cast(f_area_id as string))>=6 and substr(cast(f_area_id as string),5,2)='00'  then (substr(cast(f_area_id as string),1,6)+'1')
         | else $REGION end as f_region_id,
         | if((f_product_type = 2),f_product_id,f_parent_product_id) AS f_package_id,
         | f_content_provider as f_cp_sp
         | FROM t_business_record
         | WHERE
         |  f_order_type=1 and f_pay_status =1
         | and (f_product_type = 2 or f_product_type = 1)
     """.stripMargin
    //    val businessRecordDF = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val businessRecordDF = sqlContext.sql(sql1)
    val sql =
      s"""
         |(select cast(a.area_id as char) as f_region_id,a.area_name as f_region_name,
         |a.city_id as f_city_id,c.city_name as f_city_name,
         |c.province_id as f_province_id,p.province_name as f_province_name
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |where a.city_id=${regionCode} or c.province_id=${regionCode}
         |) as region
        """.stripMargin
    val regionDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    //    val regionDF = sqlContext.sql(sql)
    val sql2 =
      s"""
         |(select '$day' as f_date,
         |package_id as f_package_id,package_type as f_package_type
         |from program_package
         |) as aa
      """.stripMargin
    val packageTypeDF = DBUtils.loadMysql(sqlContext, sql2, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val df = userDF.join(businessRecordDF, Seq("f_order_id"))
    val df1 = df.join(regionDF, Seq("f_region_id"))
    val df2 = df1.join(packageTypeDF, Seq("f_package_id"))
    val df3 = df2.registerTempTable("t_business_basic_by_day")
    //DemandMeizi.delMysql(date, cn.ipanel.common.Tables.t_business_basic)
    //DBUtils.saveToHomedData_2(df3, "t_business_basic")
    //    df2.registerTempTable("basic")
    //    val sql3 =
    //      s"""
    //         |insert overwrite table t_business_basic_by_day partition(day='$date',month='$month',year='$year')
    //         |select f_package_id,f_region_id,f_order_id,price,
    //         |f_user_type,f_cycle,f_user_id,f_package_name,f_cp_sp,
    //         |f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,f_package_type
    //         |from basic
    //         |""".stripMargin
    //    writeToHive(sql3, sqlContext, cn.ipanel.common.Tables.t_business_basic_by_day, date)
    val business = sqlContext.sql(
      s"""
         |select  '$day' as f_date,
         |f_region_id,f_region_name,
         |f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_package_type,f_user_type,count(*) as f_count,
         |count(distinct(f_user_id)) as f_user_count,
         |sum(price) as f_price
         |from
         |t_business_basic_by_day
         |group by
         |f_region_id,f_region_name,
         |f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id,f_package_name,f_cp_sp,
         |f_package_type,f_user_type
          """.stripMargin)
    // DemandMeizi.delMysql(date, cn.ipanel.common.Tables.t_unsubscribe_by_day)
    DBUtils.saveToHomedData_2(business, cn.ipanel.common.Tables.t_unsubscribe_by_day)
  }


}
