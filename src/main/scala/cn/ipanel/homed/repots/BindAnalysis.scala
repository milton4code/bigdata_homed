package cn.ipanel.homed.repots

import cn.ipanel.common._
import cn.ipanel.etl.LogConstant
import cn.ipanel.utils.LogTools._
import cn.ipanel.utils.{DBUtils, DateUtils}

case class BindInfo(f_date: String, f_bind_time: String, f_userid: String, f_deviceid: String, f_user_name: String, f_nick_name: String,
                    f_bind_type: String, f_cardid: String, f_customer_code: String)

case class LoginInfo(f_deviceid: String, f_app_version: String)

/**
  * 日新增绑卡用户
  */
object BindAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkSession = new SparkSession("BindAnalysis")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }
    val pathiusm = LogConstant.IUSM_LOG_PATH + day
    val lines = sc.textFile(pathiusm).map(x => x.split(" - ")).filter(x => {
      val Info = x(2)
      Info.startsWith("BindSuccess") || Info.startsWith("LoginSuccess")
    }).cache()

    import hiveContext.implicits._
    val bindDF = lines.map(x => {
      val time = x(0).substring(x(0).indexOf("]") + 1, x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-", "")
      val f_bind_time = time
      val map_p = parseMaps(x(2))
      val f_userid = map_p.getOrElse("UserId", "").toString //用户id
      val f_deviceid = map_p.getOrElse("DeviceId", "").toString //设备id
      val f_user_name = map_p.getOrElse("UserName", "").toString //用户名字
      val f_nick_name = map_p.getOrElse("NickName", "").toString //用户昵称
      val f_bind_type = map_p.getOrElse("BindType", "").toString
      val f_cardid = map_p.getOrElse("CardId", "").toString //CA 卡号
      val f_customer_code = map_p.getOrElse("CustomerCode", "").toString //绑定卡的客户证号
      //ca卡号
      //val deviceNo = map_p.getOrElse("DeviceNo", "").toString //客户证号
      BindInfo(f_date, f_bind_time, f_userid, f_deviceid, f_user_name, f_nick_name, f_bind_type, f_cardid, f_customer_code)
    }).filter(x => x.f_bind_type == "3").toDF()
    //统计用户设备号和app版本
    val loginDF = lines.map(x => {
      val map_p = parseMaps(x(2))
      val f_deviceid = map_p.getOrElse("DeviceID", "").toString
      val f_app_version = map_p.getOrElse("AppVersion", "").toString
      LoginInfo(f_deviceid, f_app_version)
    }).filter(x => x.f_app_version != "").distinct().toDF()
    //    loginDF.show()
    val userInfosql ="(select DA as f_userid,f_reg_source, create_time as f_create_time from account_info) as account "
    val lasttimeInfosql = "(select DA as f_userid,min(last_refresh_time) as f_first_login_time from account_token GROUP BY DA ) as token"

    val userInfoDF = DBUtils.loadMysql(hiveContext, userInfosql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val lasttimeInfoDF = DBUtils.loadMysql(hiveContext, lasttimeInfosql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val bindLoginDF = bindDF.join(loginDF, Seq("f_deviceid")).join(userInfoDF, Seq("f_userid")).join(lasttimeInfoDF, Seq("f_userid"))
    DBUtils.saveToHomedData_2(bindLoginDF, Tables.Dalian_Bind_Login)

    sparkSession.stop()
  }
}
