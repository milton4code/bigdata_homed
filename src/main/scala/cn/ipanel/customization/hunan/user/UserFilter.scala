package cn.ipanel.customization.hunan.user

import cn.ipanel.utils.{DateUtils, UserUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * 湖南用户过滤
  * create  liujjy
  * time    2019-05-27 0027 19:00
  */
object UserFilter {
  /**
    * 获取有效用户
    * @param resultDF
    * @param nextDay
    * @param hiveContext
    * @return
    */
  def getValidateUser(resultDF: DataFrame, nextDay: String, hiveContext: HiveContext): DataFrame = {
    val day = DateUtils.dateStrToDateTime(nextDay, DateUtils.YYYY_MM_DD).toString(DateUtils.YYYYMMDD)
    val df = hiveContext.sql(s"select device_id,uniq_id,user_id from user.t_user where day <='$day'").distinct()
    //--------------数据验证代码
    //      val dateTime = DateUtils.getNowDate("yyyyMMdd-HHmm")
    //      resultDF.selectExpr("uniq_id", "device_id", "user_id") //.except(df.selectExpr("uniq_id","device_id","user_id"))
    //        .map(_.toString().replace("[", "").replace("]", ""))
    //        .repartition(1).saveAsTextFile(s"/tmp/t_all_user/$dateTime")
    // --------------数据验证代码

    resultDF.alias("a").join(df, Seq("uniq_id")).selectExpr("a.*")
  }


  /**
    * 家庭开户有效用户过滤
    *
    * @param homeInfoDF
    * @param day   yyyyMMdd
    * @param hiveContext
    * @return
    */
  def getHomeOpenUser(homeInfoDF: DataFrame, day: String, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val nextDay = DateUtils.getAfterDay(day,targetFormat = DateUtils.YYYY_MM_DD)

    val userDF = UserUtils.getHomedUserDevice(hiveContext, nextDay)
    userDF.alias("a").join(homeInfoDF.alias("b"), $"a.home_id" === $"b.f_home_id")
      .selectExpr("a.account_name as f_account_name","b.f_open_account_time","b.f_status","b.f_home_id"
        ,"b.f_home_name","b.f_group_ids","b.f_member_count","a.address_name as f_address_name",
      "a.region_id f_region_id","a.city_id f_city_id","a.province_id as f_province_id",
      "a.region_name f_region_name","a.city_name as f_city_name","a.province_name as f_province_name")

  }


}
