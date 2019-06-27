package cn.ipanel.homed.repots

import java.sql.DriverManager

import cn.ipanel.common._
import cn.ipanel.etl.LogConstant
import cn.ipanel.utils.LogTools._
import cn.ipanel.utils.{DBUtils, DateUtils}

case class loginSuccessDF(f_date:String,f_userid:String,f_device_type:String,f_deviceid:String,f_app_version:String)
case class thirdLoginSuccessDF(f_date:String,f_userid:String,f_device_type:String,f_deviceid:String,f_app_version:String)
case class loginSuccessAnotherDF(f_date:String,f_userid:String,f_device_type:String,f_deviceid:String,f_app_version:String,f_groupid:String,f_cardid:String)
case class bossloginSuccessDF(f_date:String,f_userid:String,f_device_type:String,f_app_version:String,f_groupid:String,f_cardid:String,f_deviceid:String)
case class mobileSuccessDF(f_date:String,f_userid:String,f_deviceid:String,f_user_name:String,f_nick_name:String,f_cardid:String)
/**
  * 日新增用户
  */
object NewAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("NewAnalysis", CluserProperties.SPARK_MASTER_URL)
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    //    val hiveContext = new HiveContext(sc)
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }
    val pathiusm = LogConstant.IUSM_LOG_PATH+day

    val lines = sc.textFile(pathiusm).map(x => x.split(" - ")).filter(x => {
      val Info = x(2)
      Info.startsWith("ThirdLoginSuccess")||Info.startsWith("LoginSuccess")||Info.startsWith("MobileLoginSuccess")
    }).cache()
      val loginSuccessRDD= lines.filter(x => {
      val Info = x(2)
        Info.startsWith("LoginSuccess")
    })
    val thirdLoginSuccessRDD= lines.filter(x => {
      val Info = x(2)
      Info.startsWith("ThirdLoginSuccess")
    })
    val mobileLoginSuccessRDD= lines.filter(x => {
      val Info = x(2)
      Info.startsWith("MobileLoginSuccess")
    })

    import hiveContext.implicits._
    //含天途云激活
    val loginSuccess = loginSuccessRDD.map(x=>{
      val time = x(0).substring(x(0).indexOf("]") + 1, x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-", "")
      val map_p = parseMaps(x(2))
      val f_device_type = map_p.getOrElse("DeviceType","").toString
      val f_userid = map_p.getOrElse("DA",map_p.getOrElse("UserId","")).toString
      val f_app_version = map_p.getOrElse("AppVersion","").toString
      val f_homeid = map_p.getOrElse("HomeID","").toString
      val f_deviceid = map_p.getOrElse("DeviceID","").toString
      loginSuccessDF(f_date,f_userid,f_device_type,f_deviceid,f_app_version)
    }).filter(x=>x.f_device_type!="2").distinct().toDF()
//    loginSuccess.show()


    //第三方账户
    val thirdLoginSuccess = thirdLoginSuccessRDD.map(x=>{
      val time = x(0).substring(x(0).indexOf("]") + 1, x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-", "")
      val map_p = parseMaps(x(2))
      val f_device_type = map_p.getOrElse("DeviceType","").toString
      val f_userid = map_p.getOrElse("DA",map_p.getOrElse("UserId","")).toString
      val f_app_version = map_p.getOrElse("AppVersion","").toString
      val f_deviceid = map_p.getOrElse("DeviceID","").toString
      thirdLoginSuccessDF(f_date,f_userid,f_device_type,f_deviceid,f_app_version)
    }).filter(x=>daSourceExsits(x.f_userid)).distinct().toDF()


    //极速云激活 groupId=8 deviceType!=2
    val loginSuccessAnother = loginSuccessRDD.map(x=>{
      val time = x(0).substring(x(0).indexOf("]") + 1, x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-", "")
      val map_p = parseMaps(x(2))
      val f_device_type = map_p.getOrElse("DeviceType","").toString
      val f_userid = map_p.getOrElse("DA",map_p.getOrElse("UserId","")).toString
      val f_app_version = map_p.getOrElse("AppVersion","").toString
      val f_deviceid = map_p.getOrElse("DeviceID","").toString
      val f_homeid = map_p.getOrElse("HomeID","").toString
      val f_groupid = map_p.getOrElse("GroupID","").toString
      val f_cardid = map_p.getOrElse("SmartCard","").toString
      loginSuccessAnotherDF(f_date,f_userid,f_device_type,f_deviceid,f_app_version,f_groupid,f_cardid)
    }).filter(x=>x.f_groupid=="8"&&x.f_device_type!="2"&&daSourceExsits(x.f_userid)&&x.f_cardid!="nocard").distinct().toDF()
//    loginSuccessAnother.show()



    //boss激活的数据来源
    val bossloginSuccess = loginSuccessRDD.map(x=>{
      val time = x(0).substring(x(0).indexOf("]") + 1, x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-", "")
      val map_p = parseMaps(x(2))
      val f_userid = map_p.getOrElse("DA",map_p.getOrElse("UserId","")).toString
      val f_device_type = map_p.getOrElse("DeviceType","").toString
      val f_app_version = map_p.getOrElse("AppVersion","").toString
      val f_groupid = map_p.getOrElse("GroupID","").toString
      val f_cardid = map_p.getOrElse("SmartCard","").toString
      val f_homeid = map_p.getOrElse("HomeID","").toString
      val f_deviceid = map_p.getOrElse("DeviceID","").toString
      bossloginSuccessDF(f_date,f_userid,f_device_type,f_app_version,f_groupid,f_cardid,f_deviceid)
    }).filter(x=>x.f_groupid=="11"&&x.f_cardid!="nocard").distinct().toDF()
//    bossloginSuccess.show()

    // MobileLoginSuccess(极速云激活的数据)
    val mobileSuccess = mobileLoginSuccessRDD.map(x=>{
      val time = x(0).substring(x(0).indexOf("]") + 1, x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-", "")
      val map_p = parseMaps(x(2))
      val f_userid = map_p.getOrElse("DA",map_p.getOrElse("UserId","")).toString
      val f_deviceid = map_p.getOrElse("DeviceID","").toString
      val f_user_name = map_p.getOrElse("UserName","").toString
      val f_nick_name = map_p.getOrElse("NickName","").toString
      val f_cardid = map_p.getOrElse("SmartCard","").toString
      mobileSuccessDF(f_date,f_userid,f_deviceid,f_user_name,f_nick_name,f_cardid)
    }).filter(x=>daSourceExsits(x.f_userid)).distinct().toDF()
//    mobileSuccess.show()
    val lasttimeInfosql =
      """(
        |select DA as f_userid,min(f_create_time) as f_create_time,min(expires_in) as f_first_login_time from account_token GROUP BY DA
        |) as token
      """.stripMargin
    val case_f_reg = "case f_reg_source when 1 then 'boss激活' when 3 then '家庭注册' " +
      "when 4 then 'boss宽带用户' when 5 then '手机注册' when 7 then '微信注册' when 6 then 'QQ注册'  " +
      "when 8 then '微博' when 9 then '游客' when 10 then 'boss宽带用户' when 11 then '天途云激活' " +
      "when 13 then '家庭注册' end as f_reg_source"
    val userInfosql = "(select distinct CAST(DA as char) as f_userid,CAST(create_time as CHAR) as f_device_login_time," +
      "account_name as f_user_name,nick_name as f_nick_name,f_reg_source from account_info  " +
      "where  f_reg_source in (1,3,4,5,6,7,8,9,10,11,13))  as account_info"
    val userInfoDF = DBUtils.loadMysql(hiveContext, userInfosql,DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).cache()
    val lasttimeInfoDF = DBUtils.loadMysql(hiveContext, lasttimeInfosql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).cache()
//
    val loginSuccessResult = loginSuccess.join(userInfoDF,Seq("f_userid"))
      .join(lasttimeInfoDF,Seq("f_userid"))
//    loginSuccessResult.show()
    DBUtils.saveToHomedData_2(loginSuccessResult,Tables.Dalian_New_Open)

    val thirdLoginSuccessResult = thirdLoginSuccess.join(userInfoDF,Seq("f_userid"))
  .join(lasttimeInfoDF,Seq("f_userid"))
//    thirdLoginSuccessResult.show()
    DBUtils.saveToHomedData_2(thirdLoginSuccessResult,Tables.Dalian_New_Open)

    val loginSuccessAnotherResult = loginSuccessAnother.join(lasttimeInfoDF,Seq("f_userid")).join(userInfoDF,Seq("f_userid"))

    DBUtils.saveToHomedData_2(loginSuccessAnotherResult,Tables.Dalian_New_Open)

    val bossloginSuccessResult = bossloginSuccess.join(userInfoDF,Seq("f_userid"))
  .join(lasttimeInfoDF,Seq("f_userid"))
    DBUtils.saveToHomedData_2(bossloginSuccessResult,Tables.Dalian_New_Open)

    val mobileSuccessResult = mobileSuccess.join(userInfoDF.drop("f_user_name").drop("f_nick_name"),Seq("f_userid"))
  .join(lasttimeInfoDF,Seq("f_userid"))
    DBUtils.saveToHomedData_2(mobileSuccessResult,Tables.Dalian_New_Open)

  }
  def daSourceExsits(DA: String): Boolean = {
    val SQL = "select * from t_new_open where f_reg_source='5' and DA='" + DA + "'"
    try {
      val connect = DriverManager.getConnection(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
      Class.forName("com.mysql.jdbc.Driver")
      val statement = connect.createStatement()
      val res = statement.executeQuery(SQL)
      val bool = if (!res.next()) false else true
      connect.close()
      bool
    } catch {
      case t: Throwable => true
    }
  }



}
