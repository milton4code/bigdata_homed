package cn.ipanel.homed.general

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils, RegionUtils}
import org.apache.spark.sql.DataFrame

/**
  * 业务营收（整体营业收入、cp/sp收入明细、订购活跃度、套餐订购量）
  * 入口参数：日期  是否清表
  *
  * @author ZouBo
  * @date 2018/1/4 0004 
  */
object BusinessRevenue {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("参数异常,格式[date]")
      System.exit(1)
    } else {

      val date = args(0) //日期
      val nextDate = DateUtils.getAfterDay(date)

      val sparkSession = SparkSession("BusinessRevenue")
      val sqlContext = sparkSession.sqlContext
      val dateTime = DateUtils.dateStrToDateTime(date,DateUtils.YYYYMMDD)
      val startDay = dateTime.toString(DateUtils.YYYY_MM_DD)
      val endDay = dateTime.plusDays(1).toString(DateUtils.YYYY_MM_DD)


      val querySql1 = s"( select * from table where invalid=0 and buy_time BETWEEN '$startDay' and '$endDay' ) t"
      val user_pay_infoDF = MultilistUtils.getMultilistData("homed_iusm","user_pay_info",sqlContext,querySql1)
      user_pay_infoDF.registerTempTable("user_pay_info")

      val querySql2 = s"( select * from table where  f_order_time >'$startDay' and f_order_time<'$endDay' ) t"
      val t_business_recordDF = MultilistUtils.getMultilistData("homed_iusm","t_business_record",sqlContext,querySql2)
      t_business_recordDF.registerTempTable("t_business_record")
      t_business_recordDF.count()


      //项目部署地code(省或者地市)
      var regionCode = RegionUtils.getRootRegion
      var regionTag = "" // 截取标志
      var len = 0
      if (regionCode.endsWith("0000")) {
        regionCode = (regionCode.toInt + 100).toString
        regionTag = regionCode.substring(0, 2) //部署省 截取省标志
        len = 2
      } else {
        regionTag = regionCode.substring(0, 4) //部署市 截取市标志
        len = 4
      }
      val regionBC = sparkSession.sparkContext.broadcast(regionCode)
      //存在重复处理数据情况，因此先清表
      delMySql(date)

      //加载数据
      val querybusiness =
        s"""
           |select cast(t.f_order_id as string) as f_order_id,t.f_device_id,t.f_device_type,t.f_product_id,t.f_product_type,
           |t.f_parent_product_id,t.f_order_type,
           |regexp_replace(substr(t.f_order_time, 1,10),"-","") as f_order_time,hour(t.f_order_time) as f_hour,
           |t.f_payment_mode,t.f_content_provider,
           |t.f_order_price,
           |case when length(cast(f_area_id as string))>6 and substr(f_area_id,1,$len)=$regionTag then substr(f_area_id,1,6)
           | when length(cast(f_area_id as string))=6 and substr(f_area_id,5)!='00'  then f_area_id
           | when length(cast(f_area_id as string))=6 and  substr(f_area_id,5)='00'  then  f_area_id+1
           | else ${regionBC.value.toInt + 1} end as f_area_id,
           |cast(f_pay_user_id as string) as f_pay_user_id,
           |(case when t.f_product_type=1 then t.f_parent_product_id else t.f_product_id end) as f_package_id
           |from t_business_record t
           |where t.f_order_time BETWEEN '$startDay' and '$endDay'
           |and t.f_pay_status=${UserActionType.BUSINESS_USER_PAY_SUCCESS.toInt}
           |and t.f_order_type=${UserActionType.BUSINESS_ORDER_TYPE_CONSUME.toInt}
           |and t.f_payment_mode<${UserActionType.BUSINESS_DISCOUNT_COUPON.toInt}
           |
          """.stripMargin

      val region =
        s"""
           |(select a.area_id,a.area_name,a.city_id,c.city_name,c.province_id,p.province_name
           |from (area a left join city c on a.city_id=c.city_id)
           |left join province p on c.province_id=p.province_id
           |) as region
        """.stripMargin

      // 加入订购记录有效判断或昨天失效判断
      //获取 f_order_id,f_buy_time ,f_hour_pay,f_reorder,f_service_provider,price
      val querypaySql =
      s"""
         |select u.f_order_id as f_pay_order_id,regexp_replace(u.buy_time,"-","") as f_buy_time,
         | hour(u.buy_time) as f_hour_pay,u.invalid,u.f_reorder,u.f_service_provider as f_cp_sp,u.price,
         | u.effect_time,u.exp_date,u.f_area_id as f_pay_area_id
         | from user_pay_info u
         | where u.invalid=0
         |
        """.stripMargin

//      val businessDF = DBUtils.loadMysql(sqlContext, querybusiness, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      val businessDF = sqlContext.sql(querybusiness)
      val areaDF = DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
//      val payDF = DBUtils.loadMysql(sqlContext, querypaySql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      val payDF = sqlContext.sql(querypaySql)
      val bussPayDF = businessDF.join(payDF, businessDF("f_order_id") === payDF("f_pay_order_id"), "left_outer").drop("f_pay_order_id")
      val bussPayAreaDF = bussPayDF.join(areaDF, bussPayDF("f_area_id") === areaDF("area_id")).drop("f_area_id").cache()
      process(bussPayAreaDF, areaDF, payDF, sparkSession, date, nextDate)

      user_pay_infoDF.unpersist()
      bussPayAreaDF.unpersist()
    }

  }

  /**
    * 先清理表中数据
    */
  def delMySql(date: String): Unit = {

    delMysql(date, Tables.t_business_operation_revenue_ordercontent)
    delMysql(date, Tables.t_business_operation_revenue_region)
    delMysql(date, Tables.t_business_operation_revenue_paytype)
    delMysql(date, Tables.t_business_operation_revenue_order_liveness)
    delMysql(date, Tables.t_business_cp_sp_incoming)
    delMysql(date, Tables.t_business_cp_sp_ordercontent)
    delMysql(date, Tables.t_business_operation_package_sales)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  /**
    * 业务处理逻辑
    *
    * @param bussPayAreaDF
    * @param areaDF
    * @param payDF
    * @param sparkSession
    * @param daytime
    * @param nextDate
    */
  def process(bussPayAreaDF: DataFrame, areaDF: DataFrame, payDF: DataFrame, sparkSession: SparkSession, daytime: String, nextDate: String) = {
    val sqlContext = sparkSession.sqlContext
    //获取套餐信息
    //package_type 点播 回看 时移 应用 综合 直播
    //f_order_type 0 整包购买 1 剧集购买
    val queryPackage =
    """(Select package_id,package_name,package_type,f_order_type as package_f_order_type
      |from program_package) as package
    """.stripMargin
    val packageDF = DBUtils.loadMysql(sqlContext, queryPackage, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val df = bussPayAreaDF.join(packageDF, bussPayAreaDF("f_package_id") === packageDF("package_id"), "left_outer").drop("package_id")
    val businessOperationDF = businessOperation_2(sparkSession, df, daytime)
    //    // 订购活跃度（新增到期）
    //   //   businessOperationDF.show(100)
    val orderLivness = orderLivenesOrderExp(areaDF, payDF, sparkSession, daytime, nextDate)
    //   //   orderLivness.show(100)
    processDF(sparkSession, businessOperationDF, orderLivness)

    /** 基础表mysql */
    //    DBUtils.saveToHomedData_2(businessOperationDF, "t_business_operation")
    //    DBUtils.saveToHomedData_2(orderLivness, "t_business_operation")
    //基础表数据保存至hbase
    //    DBUtils.saveDataFrameToPhoenixNew(businessOperationDF, Tables.t_business_operation)
    //    DBUtils.saveDataFrameToPhoenixNew(orderLivness, Tables.t_business_operation)
  }

  def processDF(sparkSession: SparkSession, businessDF: DataFrame, orderLivnessExpDF: DataFrame) = {
    val hiveContext = sparkSession.sqlContext
    businessDF.registerTempTable("t_business")
    orderLivnessExpDF.registerTempTable("t_business_lineness_exp")

    //1.整体营业收入（按订体营业收入（按订购内容聚合）
    val orderContentSql =
      """
        |select
        |f_date,sum(f_order_price) as f_order_price,f_order_content
        |from t_business
        |where f_order_change_type=1 or f_order_change_type=4
        |group by f_date,f_order_content
      """.stripMargin
    val orderContentDF = hiveContext.sql(orderContentSql)

    //2. 整体营业收入（按区域聚合）
    val regionSql =
      """
        |select
        |f_date,sum(f_order_price) as f_order_price,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name
        |from t_business
        |where f_order_change_type=1 or f_order_change_type=4
        |group by f_date,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name
      """.stripMargin
    val regionDF = hiveContext.sql(regionSql)

    //3.整体营业收入（按支付方式）
    val payTypeSql =
      """
        |select
        |f_date,sum(f_order_price) as f_order_price,f_order_type
        |from t_business
        |where f_order_change_type=1 or f_order_change_type=4
        |group by f_date,f_order_type
      """.stripMargin
    val payTypeDF = hiveContext.sql(payTypeSql)

    //4.套餐销量，合并整体营收，cpsp四张表
    val packageSals =
      """
        |select
        |f_date,sum(f_order_price) as f_order_price,count(f_package_id) as f_package_sale_count,
        |f_package_id,f_package_name,f_package_type,f_cp_sp
        |from t_business
        |where f_order_change_type=1 or f_order_change_type=4
        |group by f_date,f_package_id,f_package_name,f_package_type,f_cp_sp
      """.stripMargin

    val packageSalesDF = hiveContext.sql(packageSals)

    //6.整体营业收入(订购活跃度)
    val orderLivenessSql =
      """
        |select
        |f_date,sum(f_order_price) as f_order_price,count(1) as f_order_liveness_count,
        |f_order_change_type
        |from t_business
        |group by f_date,f_order_change_type
      """.stripMargin
    val orderLivenessDF = hiveContext.sql(orderLivenessSql)
    //7.整体营业收入(订购活跃度_新增到期)
    val orderLivenessExpSql =
      """
        |select
        |f_date,sum(f_order_price) as f_order_price,count(1) as f_order_liveness_count,
        |f_order_change_type
        |from t_business_lineness_exp
        |group by f_date,f_order_change_type
      """.stripMargin
    val orderLivenessExpDF = hiveContext.sql(orderLivenessExpSql)

    //8.cp/sp营业收入（用于营业占比和销售变化）
    val cpspIncomesql =
      """
        |select f_date,sum(f_order_price) as f_order_price,count(1) as f_count,
        |f_cp_sp
        |from t_business
        |where f_order_change_type=1 or f_order_change_type=4
        |group by f_date,f_cp_sp
      """.stripMargin
    val cpspIncomeDF = hiveContext.sql(cpspIncomesql)

    //9.cp/sp营业收入（按订购内容）
    val cpspOrderContentSql =
      """
        |select f_date,sum(f_order_price) as f_order_price,f_cp_sp,count(1) as f_count,
        |f_order_content
        |from t_business
        |where (f_order_change_type=1 or f_order_change_type=4)
        |and f_order_content is not null
        |group by f_date,f_cp_sp,f_order_content
      """.stripMargin
    val cpspPayTypeDF = hiveContext.sql(cpspOrderContentSql)


    DBUtils.saveToHomedData_2(orderContentDF, Tables.t_business_operation_revenue_ordercontent)
    DBUtils.saveToHomedData_2(regionDF, Tables.t_business_operation_revenue_region)
    DBUtils.saveToHomedData_2(payTypeDF, Tables.t_business_operation_revenue_paytype)
    DBUtils.saveToHomedData_2(orderLivenessDF, Tables.t_business_operation_revenue_order_liveness)
    DBUtils.saveToHomedData_2(orderLivenessExpDF, Tables.t_business_operation_revenue_order_liveness)
    DBUtils.saveToHomedData_2(cpspIncomeDF, Tables.t_business_cp_sp_incoming)
    DBUtils.saveToHomedData_2(cpspPayTypeDF, Tables.t_business_cp_sp_ordercontent)
    DBUtils.saveToHomedData_2(packageSalesDF, Tables.t_business_operation_package_sales)
  }

  /**
    * 业务营收基础数据获取
    *
    * @param sparkSession
    * @param dataFrame
    * @param daytime
    * @return
    */
  def businessOperation_2(sparkSession: SparkSession, dataFrame: DataFrame, daytime: String) = {
    val hiveContext = sparkSession.sqlContext
    dataFrame.registerTempTable("business")
    //package_type 点播 回看 时移 应用 综合 直播
    // f_order_change_type 1 新增 4 续订
    //f_package_type 0 整包购买 1 剧集购买
    // f_order_type 订购类别 1:余额 ，2:银行卡，3:银联卡，4:微信,5:支付宝，6:苹果支付
    val sql =
    s"""
       |select f_order_time as f_date,
       |f_order_id,area_id as f_region_id,area_name as f_region_name,
       |city_id as f_city_id,city_name as f_city_name,
       |province_id as f_province_id,province_name as f_province_name,
       |f_order_price,f_order_time,f_hour,f_device_type as f_order_platform,
       |f_package_id,package_name as f_package_name,package_type as f_order_content,
       |(case when f_payment_mode=1 then 1 else 2 end) as f_payment_type,
       |(case when f_payment_mode=1 then 1 else f_payment_mode end) as f_order_type,
       |(case when f_payment_mode=1 then 0 else f_payment_mode end) as f_third_pay_detail,
       |f_content_provider as f_cp_sp,
       |(case when f_reorder=1 then 4 else 1 end ) as f_order_change_type,
       |package_f_order_type as f_package_type,
       |f_pay_user_id as f_pay_user_id,
       |0 as f_is_package_exp,'' as f_package_exp_date,1 as f_package_sale_count
       |from business
       |where invalid=0 or invalid is null
      """.stripMargin
    val basicdf = hiveContext.sql(sql)
    //DBUtils.saveDataFrameToPhoenixNew( basicdf,"t_business_operation")
    basicdf
  }


  /**
    * 订购活跃度（新增到期）
    *
    * @param areaDF
    * @param payDF
    * @param sparkSession
    * @param daytime
    * @param nextDate
    * @return
    */
  def orderLivenesOrderExp(areaDF: DataFrame, payDF: DataFrame, sparkSession: SparkSession, daytime: String, nextDate: String) = {
    val sqlContext = sparkSession.sqlContext
    val daytimeYyyy_mm_dd = DateUtils.transformDateStr(daytime)
    val nextDateYyyy_MM_DD = DateUtils.transformDateStr(nextDate)
    payDF.registerTempTable("t_user_pay_info")
    val sql =
      s"""
         |select '$daytime' as f_date,f_buy_time as f_order_time,f_hour_pay as f_hour,f_cp_sp,price,exp_date,
         |f_pay_area_id,3 as f_order_change_type,f_pay_order_id as f_order_id
         |from t_user_pay_info
         |where exp_date between '$daytimeYyyy_mm_dd' and '$nextDateYyyy_MM_DD'
      """.stripMargin

    val resultDF = sqlContext.sql(sql)
    resultDF.join(areaDF, resultDF("f_pay_area_id") === areaDF("area_id"), "left_outer").drop("area_id").registerTempTable("order_liness")

    val resultSQL =
      """
        |select f_date,f_order_time,f_hour,f_order_id,province_id as f_province_id,province_name as f_province_name,
        |city_id as f_city_id,city_name as f_city_name,f_pay_area_id as f_region_id,area_name as f_region_name
        |,price as f_order_price,f_cp_sp,f_order_change_type,1 as f_is_package_exp,
        |cast(exp_date as string) as f_package_exp_date
        |from order_liness
      """.stripMargin

    sqlContext.sql(resultSQL)
  }

}
