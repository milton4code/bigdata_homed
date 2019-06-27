package cn.ipanel.utils

import cn.ipanel.common._
import cn.ipanel.customization.hunan.user.UserFilter
import cn.ipanel.customization.wuhu.users.UsersProcess
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext

/**
  * 用户工具类
  * create  liujjy
  * time    2019-05-08 0008 11:44
  */
object UserUtils {

  /**
    * 获取家庭主账号信息<p>
    * 主要用于确保用户总量中的统计数据唯一性.
    * @param hiveContext
    * @return
    */
  def getHomeInfo(hiveContext: HiveContext,nextDay:String):DataFrame={
    val homeInfoSql =
      s"""
         |(SELECT home_id,master_account as user_id,f_home_attribute
         |from home_info
         | where  master_account >0 and f_create_time < '$nextDay') t
       """.stripMargin
    DBUtils.loadMysql(hiveContext, homeInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }


  /**
    * 获取有效区域码
    * @param region  region
    */
  def getValidateRegion(region:String):String={
    var region_id = ""
    val rootRegion = RegionUtils.getRootRegion
    val (rProCode,rCityCode) = (rootRegion.substring(0,2),rootRegion.substring(0,4))

    val _region = region.toString.trim
    //第一步判断数据库中的区域码是有效性
    if(_region.length >=6 ) {
      region_id = _region.substring(0,6)
    }else if(_region.length < 6) {
      region_id = RegionUtils.getDefaultRegion()
    }
    //特殊情况:数据库中保存到的数据为4401001112 这种,截取前六位数据依然是错误的
    if(region_id.endsWith("00")){
      region_id = region_id.substring(0,4)+ "01"
    }

    val (proCode,cityCode) = (region_id.substring(0,2),region_id.substring(0,4))

    if(Constant.REGION_VERSION_PROVICE.equals(CluserProperties.REGION_VERSION)){
      //第二步判断数据库中的区域是否在部署地区内
      if(rootRegion.endsWith("0000") && rProCode != proCode){//省中心
        region_id = rProCode + "0101"
      }else if( !rCityCode.endsWith("00") && rCityCode != cityCode) { //地级市
        region_id =  rProCode + "0101"
      }
    }
    //    print("region_id====> " + region_id)
    region_id
  }


  /**
    * 获取homed 用户和设备之前的关系
    *
    * @param nextDay 日期格式为 YYYY-MM-DD
    * @return user_id|f_reg_source|home_id|account_name|nick_name|province_id|city_id|region_id|province_name|city_name|region_name
    *         |device_id|device_type|uniq_id|address_name|create_time
    */
  def getHomedUserDevice(hiveContext: HiveContext, nextDay: String): DataFrame = {
    val homedUserDF = getAllHomedUsers(hiveContext, nextDay)
    val deviceCaDF = DeviceUtils.getDeviceCaInfo(hiveContext, nextDay)

    val filter = CluserProperties.STATISTICS_TYPE match {
      case StatisticsType.STB => "device_type in ('1','2')"
      case StatisticsType.MOBILE => "device_type not in ('1','2')"
      case StatisticsType.ALL => " 1=1 "
    }

    val resultDF = homedUserDF.alias("a").join(deviceCaDF.alias("b"), Seq("home_id"))
      .filter(filter)
      .selectExpr("a.*", "b.device_id", "b.device_type", "b.uniq_id")
      .filter("device_id != 0 ")

     CluserProperties.REGION_CODE match {
      case Constant.HUNAN_CODE =>
        UserFilter.getValidateUser(resultDF,nextDay,hiveContext)
      case Constant.JILIN_CODE =>
        UserFilter.getValidateUser(resultDF,nextDay,hiveContext)
      case _ =>   resultDF
    }

  }


  /**
    * 获取所有homed用户
    *
    * @param day 日期格式为 YYYY-MM-DD
    * @return user_id|f_reg_source|home_id|account_name|nick_name|
    *         province_id|city_id|region_id|province_name|city_name|region_name|
    *         address_name|create_time|mobile_no|certificate_no
    */
  def getAllHomedUsers(hiveContext: HiveContext, day: String): DataFrame = {
    hiveContext.udf.register("region", getValidateRegion _)
    val userDF = getAllUsers(hiveContext: HiveContext, day: String)
    val addressInfoSql =
      s"""
         |(SELECT home_id , cast(region_id as char)  region_id,address_name
         |  from  address_info
         |  where status=1 and f_create_time<='$day'
         |   ) t3
         |""".stripMargin

    val addressInfoDF = DBUtils.loadMysql(hiveContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      .selectExpr("home_id", "region(region_id) region_id", "address_name")

    val regionDF = RegionUtils.getRegions(hiveContext)

    userDF.join(addressInfoDF, Seq("home_id")).join(regionDF, Seq("region_id"))
      .selectExpr("user_id", "f_reg_source", "home_id", "account_name", "nick_name"
        , "province_id", "city_id", "region_id", "province_name", "city_name", "region_name",
        "address_name","create_time","mobile_no","certificate_no")
  }

  /**
    * 获取Token数据
    *
    * @param hiveContext hiveContext
    * @param day yyyy-MM-dd 格式日期
    * @return device_id|device_type|user_id|rovince_id|city_id|region_id
    */
  def getTokenUser(hiveContext: HiveContext, day: String): DataFrame = {
    //    val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD).plus(1).toString(DateUtils.YYYY_MM_DD)
    import hiveContext.implicits._
    val querySql = s"( SELECT distinct(access_token)  access_token ,DA from table where f_create_time<'$day') t"
    val tokenDF = MultilistUtils.getMultilistData("homed_iusm", "account_token", hiveContext, querySql)
    val historyTokenDF = hiveContext.sql("select  distinct(access_token) access_token , DA from bigdata.account_token")
    val tokenUser = historyTokenDF.unionAll(tokenDF.select(historyTokenDF.columns.map(col): _*)).distinct()
      .map(row => {
        TokenParser.parserAsUser(row.getString(0))
      }).toDF().filter("DA <> '' ").drop("city_id").drop("province_id")
      .dropDuplicates(Seq("DA", "device_id"))
      .withColumnRenamed("DA", "user_id")

    tokenUser
  }

  /**
    * 获取登录过平台的非游客用户数据
    *
    * @param hiveContext hiveContext
    * @param day yyyy-MM-dd 格式日期
    * @return user_id|f_reg_source|home_id|account_name|nick_name|
    *         province_id|city_id|region_id|province_name|city_name|device_id|device_type|region_id|
    *         device_id|device_type
    */
  def getValidateUser(hiveContext: HiveContext, day: String): DataFrame = {
    /*
    +--------+------------+-------+------------+---------+-----------+-------+---------+-------------+---------+----------+-----------+---------+
    | user_id|f_reg_source|home_id|account_name|nick_name|province_id|city_id|region_id|province_name|city_name| device_id|device_type|region_id|
    +--------+------------+-------+------------+---------+-----------+-------+---------+-------------+---------+----------+-----------+---------+
    |50000162|           1|    162|    chenhuan|    陈欢35|     440000| 440100|   440118|        广东省|    广州市|1000000162|          1|   440303|

     */
    //过滤掉token中游客数据,同时也过滤掉了homed数据库中之开户但没登录行为的僵尸用户
    val homedUserDF = getAllHomedUsers(hiveContext, day)
    val tokenUserDF = getTokenUser(hiveContext, day)
    homedUserDF.alias("a").join(tokenUserDF.alias("b"), Seq("user_id"))
      .selectExpr("a.*", "b.device_id", "b.device_type")
  }


  /**
    * 获取移动端注册绑定CA卡关系的数据
    * 这个功能目前只用在番禺用户过滤中
    *
    * @param hiveContext hiveContext
    * @param nextDay 日期YYYY-MM-dd
    * @return da|ca|
    */
  def getMobileCaBinds(hiveContext: HiveContext, nextDay: String): DataFrame = {
    import hiveContext.implicits._
    val mobileCaSql =
      s"""
         |(SELECT f_da da,f_card ca
         |  FROM t_da_card
         | WHERE f_status=1 and f_updatetime < '$nextDay'
         |) mobileCa
         |""".stripMargin

    val df = DBUtils.loadMysql(hiveContext, mobileCaSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val df2 = DeviceUtils.getDeviceCaInfo(hiveContext,nextDay )

    df.alias("t1").join(df2.alias("t2"), $"t1.ca" === $"t2.uniq_id")
      .selectExpr("cast(t1.da as string) da", "t2.uniq_id", "t2.home_id", "t2.address_id", "t2.device_type")
  }

  /**
    * 获取所有用户
    * @param nextDay 日期格式为 YYYY-MM-dd
    * @return user_id|f_reg_source|home_id|account_name|nick_name|create_time|mobile_no|certificate_no
    */
  def getAllUsers(hiveContext: HiveContext, nextDay: String): DataFrame = {
    /*
     注册来源 f_reg_source 数值型
     1：boss CA用户（单个开户），
     2：homd后台注册 CA用户，
     3：终端超级用户添加CA用户，
     4：boss宽带用户（OTT），
     5：homed 终端注册（OTT）【默认】，
     6：QQ用户（OTT），
     7：微信用户（OTT），
     8：微博用户（OTT），
     9：游客用户，
     10：boss 批量导入的CA用户，
     11：boss 批量导入的广电自定义VIP用户。
      */
    //val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD).plus(1).toString(DateUtils.YYYY_MM_DD)
    //只需要正常激活用户和注册源为非游客类型的用户
    var accountInfoSql = new StringBuilder(
      s"""
         |( SELECT  DA ,account_name,nick_name,f_reg_source,home_id,create_time,mobile_no,certificate_no
         |  from  account_info
         |  where status=1 and create_time<'$nextDay'
       """.stripMargin)

    //定制化要求
    CluserProperties.REGION_CODE match {
      case Constant.HUNAN_CODE => accountInfoSql.append(" ") //代码不能缺掉,湖南不能直接过滤游客类型数据
      case Constant.WUHU_CODE => accountInfoSql.append(s" and  ").append(UsersProcess.getEffectStr(nextDay))
      case _ => accountInfoSql.append(
        s"""
           | and f_reg_source <> 9
           | and ( nick_name  not like '%${GatherType.GUEST2}%'
           |  or  nick_name  not like '%${GatherType.GUEST}%'
           |  or  account_name not like '%${GatherType.GUEST}%')
        """.stripMargin)
    }

    accountInfoSql.append(" ) t1")

    val homeInfoSql = s"(SELECT home_id, address_id from  home_info where status=1 and f_create_time <='$nextDay' ) t2"

    println(accountInfoSql.toString())
    val accontInfoDF = DBUtils.loadMysql(hiveContext, accountInfoSql.toString(), DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val homeInfoDF = DBUtils.loadMysql(hiveContext, homeInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    /*
    +--------+------------+-------+------------+-----------+-----------+-------+---------+-------------+---------+
    |      DA|f_reg_source|home_id|account_name|  nick_name|province_id|city_id|region_id|province_name|city_name|
    +--------+------------+-------+------------+-----------+-----------+-------+---------+-------------+---------+
    |50000433|           5|    433|    huangxin|       黄新35|     440000| 440300|   440303|          广东省|      深圳市|
     */
    accontInfoDF.join(homeInfoDF, Seq("home_id"))
      .selectExpr("cast(DA as string) user_id", "f_reg_source", "home_id", "account_name", "nick_name",
        "create_time","mobile_no","certificate_no")
  }


//  def main(args: Array[String]): Unit = {
//
//
//    val time = new LogUtils.Stopwatch("aap程序名称")
//
//    val sparkSession = SparkSession("UserUtils")
//    val sc = sparkSession.sparkContext
//    val sqlContext = sparkSession.sqlContext
//    val day = "20190504"
//    val day_yyyy_MM_dd = "2019-05-05"
//
//    test_getValidateRegion()
//    println(time)
//  }
//
//  private def test_regon(hiveContext: HiveContext, day: String):Unit = {
//    hiveContext.udf.register("region", getValidateRegion _)
//    val addressInfoSql =
//      s"""
//         |(SELECT home_id , cast(region_id as char)  region_id
//         |  from  address_info
//         |  where status=1 and f_create_time<='$day'
//         |   ) t3
//         |""".stripMargin
//
//    val addressInfoDF = DBUtils.loadMysql(hiveContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
//      .selectExpr("home_id","region(region_id) region_id")
//    addressInfoDF.show()
//
//  }
//
//  private def test_getValidateRegion(): Unit = {
//    //省网模式 440000
//    //    println(getValidateRegion("440100")) //正确结果应该是440101
//    //    println(getValidateRegion("44010111"))  //正确结果应该是440101
//    //        println(getValidateRegion("440201")) //正确结果应该是440201
//    //        println(getValidateRegion("44020001")) //正确结果应该是440201
//    //        println(getValidateRegion("44021")) //正确结果应该是440101
//    println(getValidateRegion("0")) //正确结果应该是440101
//
//  }
}
