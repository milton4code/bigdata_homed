package cn.ipanel.homed.general

import cn.ipanel.common.{CluserProperties, Constant, DBProperties, SparkSession}
import cn.ipanel.customization.wuhu.users.UsersProcess
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils, RegionUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
  * 用户统计报表，四个维度(来源、区域、是否付费、设备类型)
  * 数据源iusm库: account_info等7个表，一次读入，多次多维度统计使用
  * 20180129 created by wenbin
  * */
object CountUsers {
  val sqlHB = DBUtils
  val stablea = "t_da" //(个人)存入表名，蒋欣华弄的表名和表结构
  val stableh = "t_home" //(家庭)存入表名，蒋欣华弄的表名和表结构
  val stablehp = "t_home_by_pay" //(付费家庭)存入表名，蒋欣华弄的表名和表结构
  val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码 added @20190421
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("CountUsers")
    val sc=sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    if (args.size != 1) {
      println("参数错误,请输入日期 例如 20180518")
      sys.exit(-1)
    }

    val day = args(0)
    val nextDay = DateUtils.getAfterDay(day)

    val querySql1 = s"( select * from table where invalid=0 and buy_time BETWEEN '$day' and '$nextDay' ) t"
    val user_pay_infoDF = MultilistUtils.getMultilistData("homed_iusm","user_pay_info",sqlContext,querySql1)
    user_pay_infoDF.registerTempTable("user_pay_info")
    user_pay_infoDF.count()

    byArea(day,nextDay,sqlContext) //按区域统计 用户总量、付费用户表(全量重新统计)

    user_pay_infoDF.unpersist()
   sparkSession.stop()
  }

  /**
    * 防止重跑导致重复数据
    * */
  def delMysql(day:String,table:String): Unit ={
    val del_sql=s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  /**
    * (全量重新统计 新增量、当前量、删除量)
    * status=2欠费 status=3停机 都可以认为是欠费停机(忽略);
    * */
  def byArea(netss:String, nextDay:String, sqlContext:SQLContext) {
    //来源为null的是早期的账号
    var sta = new StringBuilder()
    sta.append("(")
      .append(s"""SELECT DA,home_id,status,f_reg_source,date_format(create_time,'%Y%m%d %H:%i:%s') as create_time,
                | date_format(f_status_update_time,'%Y%m%d %H:%i:%s') as f_status_update_time
                |from account_info
                |WHERE create_time< $nextDay """.stripMargin)
     if(Constant.HUNAN_CODE.equals(CluserProperties.REGION_CODE)){
         sta.append(" and nick_name != '游客' ")
     }

     sta.append(" ) as t ")

    val shome = s"(SELECT home_id ,master_account as DA,status,date_format(f_create_time,'%Y%m%d %H:%i:%s') as f_create_time,date_format(f_status_update_time,'%Y%m%d %H:%i:%s') as f_status_update_time from home_info WHERE f_create_time<$nextDay ) as t_home_info"
    val saddress = "(SELECT home_id,region_id as area_id,city_id as f_city_id,province_id as f_province_id from address_info ) as t_address"
    val scity = "(SELECT c.city_name as f_city_name,c.city_id as f_city_id,c.province_id as f_province_id,p.province_name as f_province_name from city c LEFT JOIN province p on c.province_id=p.province_id) as t_city"
    val sdistrict = "(SELECT area_id ,area_name as f_region_name,city_id as f_city_id from area ) as t_area"

    val allDa = sqlHB.loadMysql(sqlContext, sta.toString(), DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val allHome = sqlHB.loadMysql(sqlContext, shome, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val daDF =
    //地方项目定制化 - 用户

    clusterRegion match {
      case "340200" => { //芜湖
        UsersProcess.matchEffectPayHomeDf(allDa,nextDay,sqlContext)
      }
      case _ => allDa
    }
    val homeDF =
      //地方项目定制化 - 家庭
      clusterRegion match {
        case "340200" => { //芜湖
//          UsersProcess.getEffectHomeDf(nextDay,sqlContext)
          UsersProcess.matchEffectPayHomeDf(allHome,nextDay,sqlContext)
        }
        case _ => //默认做法
          allHome
      }
    val addressDF = sqlHB.loadMysql(sqlContext, saddress, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val cityDF = sqlHB.loadMysql(sqlContext, scity, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      .persist()
    // 区级地址
    val districtDF = sqlHB.loadMysql(sqlContext, sdistrict, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    val region_token=RegionUtils.getRootRegion
    //获取城市,区 code 和name
    val city_distr = districtDF.join(cityDF, Seq("f_city_id"), "left")//.persist() //加上city_name，这是左连接的基数
      .filter(s"f_city_id=$region_token or f_province_id=$region_token").persist()

    // 根据地址过滤用户
    if (city_distr.filter(s"f_city_id='$region_token'").count > 0 )
    { //当前区域为市级网
      //所有用户,home_id=0则关联不到地址信息,只含status=0/1/2/3/4
      val all_da0=daDF.join(addressDF.drop("f_province_id"), Seq("home_id"), "left").filter(s"f_city_id=$region_token").persist()

      val all_da = all_da0.filter(" area_id is not null and area_id!=0") //这过滤待定
        .drop("f_city_id").persist()
      if (all_da0.filter("area_id is null or area_id=0").count!=0)
        println("当前市网含有未知区用户！")
      val all_da_null = sqlContext.emptyDataFrame //置空省级计算
      println(DateUtils.getNowDate("yyyy-MM-dd HH:mm:ss"),"action打断惰性求值优化链all_da.count",all_da.count)
      all_da0.unpersist()
      val all_home = homeDF.join(all_da.select("DA", "home_id", "f_reg_source", "area_id"), Seq("DA", "home_id")) //注意关联到家庭主账号来源、region_id
        .persist()
      val all_home_null = sqlContext.emptyDataFrame //置空省级计算
      countByToken(all_da, all_home,all_da_null,all_home_null,cityDF, sqlContext, city_distr, netss,nextDay)
    }else if (city_distr.filter(s"f_province_id=$region_token").count >0 ){
      //当前区域为省网
      println("当前省网：",region_token)
      val all_da0=daDF.join(addressDF, Seq("home_id"), "left").filter(s"f_province_id=$region_token")
        .drop("f_city_id").drop("f_province_id")
      val all_da = sqlContext.emptyDataFrame //置空区级计算
//      val all_da_null = all_da0.withColumnRenamed("f_city_id","area_id") //有些没有area_id，就用city_id算
      val all_da_null = all_da0 //有些没有area_id，就用city_id算
        .persist()
      println(DateUtils.getNowDate("yyyy-MM-dd HH:mm:ss"),"action打断惰性求值优化链,all_da_null.count",all_da_null.count)

      val all_home = sqlContext.emptyDataFrame //置空区级计算
      val all_home_null = homeDF.join(all_da_null.select("DA", "home_id", "f_reg_source", "area_id"), Seq("DA", "home_id"),"left")
        .persist()
      countByToken(all_da, all_home,all_da_null,all_home_null,cityDF, sqlContext, city_distr, netss,nextDay)
    }else{ //未知区域级网，暂时全统计
      println("WARN: 当前非省、市级网：",region_token)
      //所有用户,home_id=0则关联不到地址信息,只含status=0/1/2/3/4
      val all_da = daDF //.select("DA","home_id","status")
        .join(addressDF, Seq("home_id"), "left").filter("area_id is not null and area_id!=0") //这过滤待定
        .drop("f_city_id").persist()
      val all_da_null = daDF.join(addressDF, Seq("home_id"), "left").filter("(area_id is null or area_id=0) and f_city_id>0") //这过滤待定
        .drop("area_id").withColumnRenamed("f_city_id","area_id") //有些没有area_id，就用city_id算
        .persist()

      val all_home = homeDF.join(all_da.select("DA", "home_id", "f_reg_source", "area_id"), Seq("DA", "home_id")) //注意关联到家庭主账号来源、region_id
        .persist()
      val all_home_null = homeDF.join(all_da_null.select("DA", "home_id", "f_reg_source", "area_id"), Seq("DA", "home_id"),"left") //这里可能要内连接
        .filter("area_id>0") //这过滤掉无city_id的，没地址信息的不统计(开发集群这里大约过滤了1/3)
        .persist()
      countByToken(all_da, all_home,all_da_null,all_home_null,cityDF, sqlContext, city_distr, netss,nextDay)
    }

  }

  def countByToken(all_da:DataFrame, all_home:DataFrame,all_da_null:DataFrame,all_home_null:DataFrame,city:DataFrame, sqlContext:SQLContext, city_distr:DataFrame, day:String,nextDay:String): Unit ={
    if (all_da.count!=0) {
      //优先按区级单位统计，只有公司集群才会市级、区级同时计算
//      println(DateUtils.getNowDate("yyyy-MM-dd HH:mm:ss"),"开始区级统计...")
      val (area_da, area_home, area_home_pay,devDa) = static(all_da, all_home, sqlContext, city_distr, day,nextDay)
      saveRegion(area_da, city_distr, day, stablea)
      saveRegion(area_home, city_distr, day, stableh)
      saveRegion(area_home_pay, city_distr, day, stablehp)
      city_distr.unpersist()
    }
    if (all_da_null.count!=0) {
      //只有城市ID的就按市级算//连城市ID都没有的话，就过滤不要了
//      println(DateUtils.getNowDate("yyyy-MM-dd HH:mm:ss"),"开始市级统计...")
      val (city_da, city_home, city_home_pay,dev_da) = static(all_da_null, all_home_null, sqlContext, city_distr, day,nextDay)
      //      println("city_da:", city_da.count)
      saveRegion(city_da, city_distr, day, stablea)
      //      println("area_user_home:", city_home.count)
      saveRegion(city_home, city_distr, day, stableh)
      //      println("city_pay:", city_home_pay.count)
      saveRegion(city_home_pay, city_distr, day, stablehp)
      all_home_null.unpersist()
      all_da_null.unpersist()
      city.unpersist()
    }
    all_home.unpersist()
    all_da.unpersist()
  }

  /**
    * 市网数据保存
    * */
  def saveRegion(df:DataFrame,city_distr:DataFrame,day:String,dbTable:String): Unit ={
    val res_df=df.join(city_distr,Seq("area_id")) //城市、区信息
      .na.fill(0)
      .withColumn("f_date",lit(day.replaceAll("-","")))
      .withColumnRenamed("area_id","f_region_id")
    delMysql(day,dbTable)
    sqlHB.saveToMysql(res_df, dbTable, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    df.unpersist()
  }

  /**
    * 省网数据保存
    * */
  def saveCity(df:DataFrame,city:DataFrame,day:String,dbTable:String): Unit ={
    val res_df=df.withColumnRenamed("area_id","f_city_id") //这里为了统一函数调用,提高代码重用性才这样重命名
      .join(city, Seq("f_city_id")) //城市名信息
//      .join(city, Seq("f_city_id"), "left") //城市名信息
      .na.fill(0)
      .withColumn("f_date",lit(day.replaceAll("-","")))
    delMysql(day,dbTable)
    //    res_df.show(2)
    sqlHB.saveToMysql(res_df, dbTable, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    df.unpersist()
  }

  /**
    * 省或者市网各维度数据统计
    * */
  def static(all_da:DataFrame,all_home:DataFrame,sqlContext:SQLContext,city_distr:DataFrame,day:String,nextDay:String):(DataFrame,DataFrame,DataFrame,DataFrame)={
    //status=0未激活 /1正常使用 /2欠费 /3暂停使用 /4注销
    val da_home01234=all_da.filter("status!=9").select("DA","home_id","status","home_id","area_id","create_time","f_reg_source").persist()
    val all_home2=all_home.filter("status!=9")

    val dev_da= sqlContext.emptyDataFrame

    //分个人、家庭 统计新注册、当前总量
    val (new_current_da,new_current_home)=newAndCurrent(da_home01234,all_home2,day,nextDay) //是否需要.filter("status!=9") ?

    //按个人、家庭 统计销户量
    val (del_da,del_home)=countDel(all_da,all_home,day,nextDay)

    //按家庭 统计停机、恢机量
    val dis_or_reuse=reuseState(all_da,all_home,day,nextDay)

    //(以下代码)按家庭 区域统计付费户数
    val all_home1=all_home.filter("status=1").persist()
    val (pay_home,new_pay)=byPay(day,nextDay,sqlContext)
    val homes1= all_home1
      .join(pay_home,Seq("home_id"),"left")

    //付费(维度)户数(过滤status=1)
    val all_home_num=all_home1.groupBy("area_id").count.withColumnRenamed("count","f_count_home1")
    val payed_home_ = homes1.selectExpr("area_id","home_id","payed").groupBy("area_id")
      .agg("payed"->"count") //null不会被agg里count计数，注意区分GroupedData.count
      .withColumnRenamed("count(payed)","f_payed_home")
      .join(all_home_num,Seq("area_id"),"left")
    //1表示免费，2是付费
    val payed_home=payed_home_
      .selectExpr("area_id","f_payed_home as f_num")
      .withColumn("f_type",lit("2"))
      .unionAll(payed_home_ //未付费家庭数f_free_home= f_count_home_id1 - f_payed_home
        .selectExpr("area_id","f_count_home1 - f_payed_home as f_num")
        .withColumn("f_type",lit("1")) )

    all_home.registerTempTable("all_home")
    val caseStr="(CASE WHEN status=0 THEN f_count_home_id ELSE  0 END  ) AS f_count_home0," +
      "(CASE WHEN status=1 THEN f_count_home_id ELSE  0 END  ) AS f_count_home1," +
      "(CASE WHEN (status=2 or status=3) THEN f_count_home_id ELSE  0 END  ) AS f_count_home23," +
      "(CASE WHEN status=4 THEN f_count_home_id ELSE  0 END  ) AS f_count_home4 "
    val all_home_id=sqlContext.sql(
      "select area_id,f_reg_source,sum(f_count_home0) as f_count_home0,sum(f_count_home1) as f_count_home1,sum(f_count_home23) as f_count_home23,sum(f_count_home4) as f_count_home4 "+
        "from ( "+
          s"select area_id,f_reg_source,$caseStr " +
          "from" +
            "(select area_id,count(home_id) as f_count_home_id,status,f_reg_source " +
            "from all_home  " +
            "where status!=9 " +
            "group by area_id,status,f_reg_source ) a " +
        ") b "+
        "group by area_id,f_reg_source")

    da_home01234.registerTempTable("da_home01234")
    val caseStrDA="(CASE WHEN status=0 THEN f_count_da ELSE  0 END  ) AS f_count_da0," +
      "(CASE WHEN status=1 THEN f_count_da ELSE  0 END  ) AS f_count_da1," +
      "(CASE WHEN (status=2 or status=3) THEN f_count_da ELSE  0 END  ) AS f_count_da23," +
      "(CASE WHEN status=4 THEN f_count_da ELSE  0 END  ) AS f_count_da4 "
    val all_da01234=sqlContext.sql(
      "select area_id,f_reg_source,sum(f_count_da0) as f_count_da0,sum(f_count_da1) as f_count_da1,sum(f_count_da23) as f_count_da23,sum(f_count_da4) as f_count_da4 "+
        "from ( "+
          s"select area_id,f_reg_source,$caseStrDA " +
          "from" +
            "(select area_id,count(DA) as f_count_da,status,f_reg_source " +
            "from da_home01234  " +
            "where status!=9 " +
            "group by area_id,status,f_reg_source ) a " +
        ") b "+
      "group by area_id,f_reg_source")

    (all_da01234 //当前01234状态个人
      .join(del_da,Seq("area_id","f_reg_source"),"left") //销户个人、家庭
      .join(new_current_da,Seq("area_id","f_reg_source"),"left") //新注册个人、家庭
      ,all_home_id
      .join(dis_or_reuse,Seq("area_id","f_reg_source"),"left") //恢机、销户
      .join(del_home,Seq("area_id","f_reg_source"),"left") //销户个人、家庭
      .join(new_current_home,Seq("area_id","f_reg_source"),"left") //新注册个人、家庭
      ,payed_home
      ,dev_da)

  }


  /** 按区域(来源)统计(非注销状态)注册家庭、人数；
    * create_time为当天的算新注册
    * */
  def newAndCurrent(all_da_home:DataFrame,all_home:DataFrame,day:String,nextDay:String):(DataFrame,DataFrame)={
    //    println("newAndCurrent")
    val new_reg=all_da_home.filter(s"create_time <='$day 23:59:59' and create_time>='$day 00:00:00'")
    val new_reg_da=new_reg.select("DA","area_id","f_reg_source")
      .groupBy("area_id","f_reg_source").agg("DA"->"count") //按来源、区域分组
      .withColumnRenamed("count(DA)","f_count_reg_new")//.persist()
    val new_reg_home=all_home.filter(s"f_create_time<='$day 23:59:59' and f_create_time>='$day 00:00:00'")
      .select("home_id","area_id","f_reg_source")//.distinct
      .groupBy("area_id","f_reg_source").agg("home_id"->"count") //按来源、区域分组
      .withColumnRenamed("count(home_id)","f_count_reg_new")//.persist()
    //下面是当前各种来源的用户量
    val cur_reg_da=all_da_home.select("DA","area_id","f_reg_source")
      .groupBy("area_id","f_reg_source").agg("DA"->"count") //按来源、区域分组
      .withColumnRenamed("count(DA)","f_count_reg_current")//.persist()
    val cur_reg_home=all_home//.filter(s"f_create_time<='$day 23:59:59' and f_create_time>='$day 00:00:00'")
      .select("home_id","area_id","f_reg_source")//.distinct
      .groupBy("area_id","f_reg_source").agg("home_id"->"count") //按来源、区域分组
      .withColumnRenamed("count(home_id)","f_count_reg_current")//.persist()

    (cur_reg_da.join(new_reg_da,Seq("area_id","f_reg_source"),"left"),
      cur_reg_home.join(new_reg_home,Seq("area_id","f_reg_source"),"left")
    )
  }

  /** 按区域(来源)统计销户家庭、人数
    * DA,home_id,status,f_reg_source,create_time,f_status_update_time
    * device_info.status=9 是设备销卡 设备少了一个创建时间,这个我们补上
    * status=4 注销 status=9删除 的都算家庭或个人销户
    *  现在代码里面也没注销状态了, =4的这个可以忽略, 用到的只有, 0,1,2,3,9 , 1正常,9删除)(或者销户),
    * 其他的0-8之间除了1之外,你都可以理解为暂停用户,
    * */
  def countDel(all_da:DataFrame,all_home:DataFrame,day:String,nextDay:String): (DataFrame,DataFrame) ={
    val del_reg_d=all_da.filter("status=4 or status=9").
      filter(s"f_status_update_time<='$day 23:59:59' and f_status_update_time>='$day 00:00:00'")
    val del_reg_h=all_home.filter("status=4 or status=9").
      filter(s"f_status_update_time <='$day 23:59:59' and f_status_update_time>='$day 00:00:00'")
    val del_reg_da=del_reg_d.select("DA","area_id","f_reg_source")
      .groupBy("area_id","f_reg_source").count.withColumnRenamed("count","f_count_del")
    val del_reg_home=del_reg_h.select("home_id","area_id","f_reg_source")
      .groupBy("area_id","f_reg_source").count.withColumnRenamed("count","f_count_del")
    (del_reg_da,del_reg_home)

  }

  /** 统计停机、恢机家庭数（来源、区域维度）
    * （家庭）恢机是当天 从状态非1 到状态1
    * 现在代码里面也没注销状态了, =4的这个可以忽略, 用到的只有, 0,1,2,3,9 , 1正常,9删除)(或者销户),
    * 其他的0-8之间除了1之外,你都可以理解为暂停用户, status=4
    * status=1: 正常, status=9: 销户, 其他状态才可以认为是停机
    * 目前C代码中没有(非1)这几个状态之间的切换, 这几个状态只能切换到1正常; 只有后台终端用户管理里面才有停机和销户的切换, 但是这个可以忽略吧,
    * */
  def reuseState(all_da_home:DataFrame,all_home:DataFrame,day:String,nextDay:String): DataFrame ={
    val update_state=all_home//.filter("status!=9")
      .filter(s"status!=9 and f_status_update_time <='$day 23:59:59' and f_status_update_time>='$day 00:00:00'")
      .persist()
    if (update_state.count==0) //没人更新状态就返回空dataframe
    {
      return update_state.select("area_id", "f_reg_source")
    }
    val all_state=update_state//.select("home_id","area_id","f_reg_source","status").distinct
      .groupBy("area_id","f_reg_source","status").agg("home_id"->"count") //
      .withColumnRenamed("count(home_id)","f_reuse_reg_home")
      .persist()
    val state1=all_state.filter("status=1") //当天恢机家庭
      .drop("status")
    val state_not1=all_state.filter("status!=1") //当天停机家庭（非1状态）
      //      .drop("status")
      .groupBy("area_id","f_reg_source").agg("f_reuse_reg_home"->"sum") //按来源、区域分组
      .withColumnRenamed("sum(f_reuse_reg_home)","f_disuse_reg_home")
    val update_home=state1.join(state_not1,Seq("area_id","f_reg_source"))//.persist()
    //    println("update_home",update_home.count)
    update_home //.show()

  }

  /** 如果 user_pay_info中针对这个家庭没有任何套餐,就是未付费
    * 查询当前付费、未付费家庭(那些付费后销户的是否也要统计?)
    * target_type=3的target_id是home_ID (无锡和宜兴订购是记录设备id的)
    *  GROUP BY target_id后buy_time = min(buy_time) 就是新增，反之是续费和新套餐
    * 返回 df(home_id,payed), df(home_id)
    * */
  def byPay(day:String,nextDay:String,sqlContext:SQLContext ): (DataFrame,DataFrame) ={
    val s="(SELECT target_id as home_id FROM user_pay_info WHERE "+ //invalid=1过期(这个似乎是实时的)的就看更新时间
      s"target_type=3 and exp_date >= $day AND ((invalid=1 and  buy_time BETWEEN $day and $nextDay ) or invalid=0) ) as t_payed"
    val payed_homeid =sqlHB.loadMysql(sqlContext, s, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      .distinct.withColumn("payed",lit("0"))
    val snew=s"(SELECT target_id as home_id,DATE_FORMAT(min(buy_time),'%Y%m%d') as buy_time	FROM	user_pay_info	where target_type=3 GROUP BY	target_id	) as t_new_pay"
    val new_pay_homeid =sqlHB.loadMysql(sqlContext, snew, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      .filter(s"buy_time='$day'").drop("buy_time").distinct() //当天成为新付费用户的homeid
    (payed_homeid,new_pay_homeid)
  }

}


