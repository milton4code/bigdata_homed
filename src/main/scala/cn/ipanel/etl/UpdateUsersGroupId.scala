package cn.ipanel.etl

import cn.ipanel.common.DBProperties
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 更新老版用户组信息
  * 只为青岛演示服务
  */
object UpdateUsersGroupId {

  def main(args: Array[String]): Unit = {
    if(args.length !=1){
      println("请输入日期:20180808")
      sys.exit(-1)
    }
    val day =args(0)
//    System.setProperty("hadoop.home.dir", "E:\\study\\hadoop\\winutils-master\\hadoop-2.6.4")

    val sc = initSpark("UpdateUsersGroupId")
    val hiveContext = new HiveContext(sc)
    val _accountInfoDF = loadDataFromMysql(hiveContext,"(SELECT  DA,home_id from account_info where status=1) a")
    val _addressInfoDF = loadDataFromMysql(hiveContext,"(SELECT  home_id ,region_id from address_info where region_id>0) b")
    val _userGroupDF = loadDataFromMysql(hiveContext,"""(SELECT  group_id ,f_regioncode region_id from user_group where f_regioncode <>"" and f_regioncode <>"0" ) c""")


    val br = sc.broadcast((_accountInfoDF,_addressInfoDF,_userGroupDF))

    val hiveDF = loadDataFromHive(hiveContext,day)
    val (accountInfoDF,addressInfoDF,userGroupDF) = br.value

    import hiveContext.implicits._
    accountInfoDF.alias("t1").join(addressInfoDF.alias("t2"),$"t1.home_id" === $"t2.home_id")
      .join(userGroupDF.alias("t3"),Seq("region_id"))
      .select("t1.DA","t3.group_id")
      .join(hiveDF.alias("t4"),$"t1.DA" === $"t4.userid","right")
      .selectExpr("t4.sevice1","t4.sevice2","t4.timehep1","t4.timehep2","t4.daytime","t4.userid","cast(group_id as string) as region","t4.terminal","t4.paras")
//      .repartition(100)
      .registerTempTable("t_tmp")

//    hiveContext.sql("select * from t_tmp").printSchema()

//    hiveContext.sql(s"alter table default.userdata_1 drop IF EXISTS partition(day='$day')")
    hiveContext.sql(
      s"""
         |insert overwrite table  default.userdata_1  partition(day='$day')
         |select sevice1,sevice2,timehep1,timehep2,daytime,userid,region,terminal,paras
         |FROM t_tmp
       """.stripMargin)

  }

  def loadDataFromHive(hiveContext: HiveContext, day: String):DataFrame={
    hiveContext.sql(
      s"""
        |select sevice1,sevice2,timehep1,timehep2,daytime,userid,region,terminal,paras
        |from default.userdata_1
        |where day=$day
      """.stripMargin)
  }

  def loadDataFromMysql(hiveContext: SQLContext, sql:String):DataFrame={
    DBUtils.loadMysql(hiveContext,sql,DBProperties.JDBC_URL_IUSM,DBProperties.USER_IUSM,DBProperties.PASSWORD_IUSM)
  }

  def initSpark(appName:String):SparkContext={
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    conf.set("spark.sql.codegen", "true")
    conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200") //小于200M就会broadcast
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    val sparkContext = new SparkContext(conf)
    sparkContext
  }
}

