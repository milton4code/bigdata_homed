package cn.ipanel.homed.repots

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession, UserActionType}
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

case class Record(f_area_id: Long, f_area: Long)

/**
  * 营收报表  t_revenue_report
  */
object RevenueReport {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("请输入有效参数,例如 20180909")
      sys.exit(-1)
    }

    val day = args(0)
    val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val time = dateTime.toString(DateUtils.YYYY_MM_DD)
    val nowtime = dateTime.plusDays(1).toString(DateUtils.YYYY_MM_DD)

    val sparkSession = SparkSession("RevenueReport", CluserProperties.SPARK_MASTER_URL)
    val hiveContext = sparkSession.sqlContext

    delMysql3(time, nowtime, cn.ipanel.common.Tables.t_revenue_report)

    val regionCode = RegionUtils.getRootRegion //默认区域码
    val RegionPro = regionCode.substring(0, 2) //省标识
    var REGION = ""
    if (regionCode.endsWith("0000")) {
      REGION = regionCode.substring(0, regionCode.length - 3) + "101" //默认市的默认区
    } else {
      REGION = regionCode.substring(0, regionCode.length - 1) + "1" //默认区
    }
    val regionInfo = Lookback.getRegionInfo(hiveContext, regionCode)
    val businessRecordDF = getBusinessRecord(time, nowtime, hiveContext, REGION, RegionPro)
    val packageTypeDF = getProgramPackage(hiveContext: HiveContext)
    getRevenueReport(time, businessRecordDF, packageTypeDF, hiveContext, regionInfo)
    println("销售量报表统计结束")
  }

  // t_business_record
  def getBusinessRecord(time: String, nowtime: String, sqlContext: HiveContext, REGION: String, regionPro: String) = {
    import sqlContext.implicits._
    val querySql = s"( select * from table where  f_order_time >'$time' and f_order_time<'$nowtime' ) t"
   val data =  MultilistUtils.getMultilistData("homed_iusm","t_business_record",sqlContext,querySql)
    data.registerTempTable("t_business_record")
//    data.count()
    val sql =
      s"""
         |SELECT f_order_id,f_order_time,f_pay_status,f_product_name,
         |(case  when  ( f_product_type = 1) then 1
         |when  (f_product_type = 2) then 2
         |when  (f_order_type = 2) then 3
         |when  (f_contenttype = 1200) then 4
         |else 5 end) as f_product_type,
         | f_payment_mode,
         | if((f_payment_mode>10000),0,f_order_price) as f_order_price,
         | f_device_type,
         | if((length(cast(f_area_id as string))>5 and substring(cast(f_area_id as string),1,2)='$regionPro'),substring(f_area_id,1,6),'$REGION') as f_area_id,
         | f_content_provider,
         | if((f_product_type = 2),f_product_id,f_parent_product_id) as package_id,
         | f_operator_id as DA,if (datediff(f_exp_date , f_effect_time) = 0,1,datediff(f_exp_date , f_effect_time)) as f_cycle
         | FROM t_business_record
         | WHERE f_order_time >'$time' and f_order_time<'$nowtime'
         | and f_order_type=${UserActionType.BUSINESS_ORDER_TYPE_CONSUME.toInt}
     """.stripMargin
    //    val businessRecordDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val businessRecordDF = sqlContext.sql(sql)
    val areaInfo = businessRecordDF.map(x => {
      val f_area_id = getAreaId(x.getAs[String]("f_area_id"))
      Record(x.getAs[String]("f_area_id").toLong, f_area_id)
    }).toDF.distinct()
    //OTT 4,5,6,7,8,9,12,13
    //有线 1,2,3,10,11,14
    val sql1 =
    s"""
       |(SELECT DA,mobile_no,
       |(case  when  (f_reg_source=4 or  f_reg_source=5 or  f_reg_source=6 or  f_reg_source=7 or  f_reg_source=8 or  f_reg_source=9 or  f_reg_source=12 or  f_reg_source=13) then 2
       |when  (f_reg_source=1 or  f_reg_source=2 or  f_reg_source=3 or  f_reg_source=10 or  f_reg_source=11 or  f_reg_source=14) then 1
       |else 3 end) as f_reg_source,f_reg_source as f_original_reg_source,home_id
       |from account_info) as aa
        """.stripMargin
    val sql_de =
      """
        |(select home_id,device_type ,f_create_time from device_info) as a
      """.stripMargin
    val businessRecordDF1 = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val userDevice = DBUtils.loadMysql(sqlContext, sql_de, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).registerTempTable("de_table") //获取用户设备类型
    val userDevice1 = sqlContext.sql(
      """
        |select home_id,device_type
        |from
        |(
        |select home_id,device_type ,f_create_time ,row_number() over(partition by home_id order by f_create_time desc) as num from de_table
        |)t
        |where num <=1
      """.stripMargin)
    // 用户和设备类型进行关联
    val user_de = businessRecordDF1.join(userDevice1, Seq("home_id"), "left").na.fill(1).drop("home_id")

    val finalDF1 = areaInfo.join(businessRecordDF, Seq("f_area_id"), "right")
    //    val finalDF = businessRecordDF1.join(finalDF1, Seq("DA"), "right")
    val finalDF = user_de.join(finalDF1, Seq("DA"), "right") //通过mysql携带设备类型
    data.unpersist()
    finalDF
  }

  //套餐类型
  def getProgramPackage(sqlContext: HiveContext): DataFrame = {
    val sql =
      """
        |(select package_id,package_type from program_package) as aa
      """.stripMargin
    val packageTypeDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    packageTypeDF
  }

  //  产品类型  套餐 f_product_type=2 时, 产品id f_product_id = package_id;
  //  单片套餐id     =1时, f_parent_product_id=package_id
  //其他是消费
  def getRevenueReport(time: String, businessRecordDF: DataFrame, packageTypeDF: DataFrame, sqlContext: HiveContext, regionInfo: DataFrame) = {
    businessRecordDF.registerTempTable("t_business")
    packageTypeDF.registerTempTable("t_package")
    val revenueReportDF = sqlContext.sql(s"select a.DA as f_da,a.mobile_no as f_mobile_no,a.f_order_id,a.f_order_time,a.f_pay_status,a.f_product_name,a.f_product_type,a.f_payment_mode,a.f_cycle,a.f_order_price," +
      s"if(a.f_device_type==0 and device_type is not null ,device_type,a.f_device_type) as f_device_type,a.f_area,a.f_content_provider," +
      "b.package_type ,a.f_reg_source,a.f_original_reg_source " +
      "from t_business a left join t_package b on a.package_id = b.package_id")
    revenueReportDF.registerTempTable("revenueReportDF")
    regionInfo.registerTempTable("t_region_info")
    import org.apache.spark.sql.functions._
    val revenueReportDF1 = sqlContext.sql(
      """
        |select a.f_da,a.f_mobile_no,a.f_order_id,a.f_order_time,a.f_pay_status,
        |a.f_product_name,a.f_product_type,a.f_payment_mode,a.f_cycle,if((a.f_cycle = 1),1,1) as f_num,
        |a.f_order_price,a.f_device_type,b.f_province_id,b.f_province_name,b.f_city_id,b.f_city_name,
        |b.f_region_id,b.f_region_name,a.f_content_provider,a.package_type as f_order_type ,a.f_reg_source,
        |a.f_original_reg_source
        | from revenueReportDF a left join t_region_info b on a.f_area = b.f_region_id
      """.stripMargin)
        .repartition(col("f_order_id"),col("f_da"))
    DBUtils.saveToHomedData_2(revenueReportDF1, cn.ipanel.common.Tables.t_revenue_report)
  }

  def getAreaId(area_id: String) = {
    var area = ""
    if (area_id.substring(5, 6) == "0") {
      area = area_id.substring(0, 5) + 1
    }
    else {
      area = area_id.substring(0, 6)
    }
    area.toLong
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql3(day: String, nowday: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_order_time>'$day' and f_order_time <'$nowday' "
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }
}
