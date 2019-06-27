package cn.ipanel.homed.repots

import cn.ipanel.common._
import cn.ipanel.etl.LogConstant
import cn.ipanel.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

case class LoginNet(f_userid:String,f_net:String)
case class appSystem(f_date:String,f_first_app_time:String,f_userid:Long,f_deviceid:Long,f_device_type:Long,f_mobile:String,f_system:String,f_app_version:String,f_server_version:String)
case class photoCountActive(f_date:String,f_userid:String,f_device_type:String,f_porgramid:Long)
case class photoCount(f_date:String,f_device_type:String,f_porgramid:Long)
case class photoInfo(f_date:String,f_userid:String,f_programid:Long,f_program_type:String,f_count:Int)
/**
  * 日活跃用户,多个维度统计用户观看电视的情况
  */
object ActiveAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("ActiveAnalysis")
    val sc = sparkSession.sparkContext
    val hiveContext = new HiveContext(sc)
    var day = DateUtils.getYesterday()
    if (args.size == 1)
      {
        day = args(0)
      }
    val pathiusm = LogConstant.IUSM_LOG_PATH+day
    import sparkSession.sqlContext.implicits._
    val lines = sc.textFile(pathiusm).map(x=>x.split(" - ")).filter(x=>{       //统计活跃的用户行为
        val Info = x(2)
      Info.startsWith("LoginSuccess")||Info.startsWith("ThirdLoginSuccess")||Info.startsWith("MobileLoginSuccess")
    }).map(x=>{
      val time = x(0).substring(x(0).indexOf("]")+1,x(0).lastIndexOf(":"))
      val timeArr = time.split(" ")
      val f_date = timeArr(0).replace("-","")
      val f_first_app_time = time
      val map_p = LogUtils.str_to_map(x(2),","," ")
      val f_userid = map_p.getOrElse("DA",null).toString.toLong //用户id
      val f_deviceid = map_p.getOrElse("DeviceID",null).toString.toLong //设备id
      val f_device_type = map_p.getOrElse("DeviceType",null).toString.toLong  //设备类型 3:手机 4:ipad
      val f_mobile = map_p.getOrElse("Mobile","").toString  //手机牌子
      val f_system = map_p.getOrElse("System","").toString //系统版本
      val f_app_version = map_p.getOrElse("AppVersion","").toString //app版本
     val f_server_version = map_p.getOrElse("Serverversion","").toString //手机版本
      appSystem(f_date,f_first_app_time,f_userid,f_deviceid,f_device_type,f_mobile,f_system,f_app_version,f_server_version)
    }).filter(x=>x.f_device_type==3||x.f_device_type==4).toDF()

    hiveContext.sql(s"use ${Constant.HIVE_DB}")
    //这里需要改一下，公司key_word中的login没有networkType 大连那边有
    val lines_base =
      s"""
         |select report_time,params['accesstoken'] as accesstoken,
         |params['networkType'] as net
         |from orc_nginx_log
         |where key_word like '%/login%' and day='$day'
            """.stripMargin
    //点击内容的id
    val program_base =
      s"""
         |select day,userid,exts['ProgramID'] as ProgramId,exts['DeviceType'] as deviceType
         |from orc_user_behavior
         |where reporttype='ProgramEnter' and day='$day'
            """.stripMargin
    val programRdd = hiveContext.sql(program_base).rdd
    val logRdd = hiveContext.sql(lines_base).rdd
    val reduceRdd =logRdd .map(x => {
        val report_time = x.getAs[String]("report_time")
        val f_net = x.getAs[String]("net")
        val accesstoken = x.getAs[String]("accesstoken")
        val arr = TokenParser.parserAsUser(accesstoken)
        val f_userid = arr.DA.toString
        //val deviceId = arr.device_id.toString
        val f_terminal = arr.device_type
      LoginNet(f_userid,f_net)
      }).toDF()
    val programDF = programRdd.map(x => {            //统计内容的点击次数
      val f_date = x.getAs[String]("day")
      val f_userid = x.getAs[String]("userid")
      val f_device_type = x.getAs[String]("deviceType")
      val f_programid = x.getAs[String]("ProgramId").toLong
      photoCountActive(f_date, f_userid, f_device_type, f_programid)
    }).filter(x => IDRangeUtils.isPhotoOrApp(x.f_porgramid))
      .map(x => {
      val key = x.f_date + "," + x.f_userid + "," + x.f_porgramid
      (key, 1)
    }).reduceByKey(_ + _).map(x => {
      val arr = x._1.split(",")
      val f_date = arr(0)
      val f_userid = arr(1)
      val f_programid = arr(0).toLong
      val f_program_type = if (f_programid > IDRangeConstant.APP_START && f_programid < IDRangeConstant.APP_END) "photo" else "app"
      val f_count = x._2
      photoInfo(f_date, f_userid, f_programid, f_program_type, f_count)
    }).toDF()

    val logInfo = lines.join(programDF,Seq("f_userid")).join(reduceRdd,Seq("f_userid"))
//    getBasicInfo(day,hiveContext,logInfo)
  }
  def getBasicInfo(day: String, hiveContext: HiveContext,logInfo:DataFrame) = {
    hiveContext.sql("use bigdata")
    //计算用户直播，点播，回看的时长和次数，由于sql不容易一次计算出来，先分别统计后，注册临时表
    val videoPlay = hiveContext.sql("select sum(case  when playType='live' then  playtime else 0 end ) as liveTime," +
      "sum(case  when playType='demand' then  playtime else 0 end ) as demandTime," +
      "sum(case  when playType='lookback' then  playtime else 0 end ) as lookbackTime," +
      "(case when playType='live' then count(*) else 0 end)as liveCount," +
      "(case when playType='demand' then count(*) else 0 end)as demandCount," +
      "(case when playType='lookback' then count(*) else 0 end) as lookbackCount," +
      "userId as DA,deviceId as device_id " +
      s"from orc_video_play where day='$day' " +
      "group by userId,deviceId,playType")
    videoPlay.registerTempTable("videoPlay_table") //注册成临时表
    val videoPlayDF = hiveContext.sql("select sum(liveTime) as f_live_time,sum(demandTime) as f_demand_time," +
      "sum(lookbackTime) as f_lookback_time,sum(liveCount) as f_live_count,sum(demandCount) as f_demand_count," +
      "sum(lookbackCount) as f_lookback_count,DA as f_userid,device_id as f_deviceid from videoPlay_table group by DA,device_id")
    //计算用户的总观看时长
    val playDayTimeDF = hiveContext.sql("select f_user_id as DA,f_device_id,f_device_type," +
      "sum(UNIX_TIMESTAMP(f_logout_time)-UNIX_TIMESTAMP(f_login_time)) as playtime " +
  s"from t_user_online_history where day='$day'" +
  "group by f_user_id,f_device_id,f_device_type  ").toDF()//提取t_user_online_history
    playDayTimeDF.show()
    val userInfosql =
    """(
      |select DA as f_userid,account_name as f_user_name,nick_name as f_nick_name,create_time as f_device_login_time,
      |f_reg_source from account_info WHERE 1=1
      |) as account
    """.stripMargin
    val deviceFirstTimesql =
    """(
      |select min(f_create_time) as f_create_time, device_id as f_deviceid from account_token GROUP BY device_id
      |) as device
    """.stripMargin
    val userVipsql=
    """(
      |SELECT f_user_id as f_userid,f_rank_id from t_user_rank
      |) as vip
    """.stripMargin
    val userInfoDF = DBUtils.loadMysql(hiveContext, userInfosql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val deviceFirstTimeDF = DBUtils.loadMysql(hiveContext, deviceFirstTimesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val userVipDF = DBUtils.loadMysql(hiveContext, userVipsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val result = videoPlayDF.join(userInfoDF,Seq("f_userid"))
      .join(playDayTimeDF,Seq("f_userid"))
      .join(deviceFirstTimeDF,Seq("f_deviceid"))
      .join(userVipDF,Seq("f_userid"))

    DBUtils.saveToHomedData_2(result,Tables.Dalian_User_Active)
  }

}
