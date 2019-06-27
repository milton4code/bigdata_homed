package cn.ipanel.common

import java.util

import cn.ipanel.utils.PropertiUtils


/**
  * 数据库属性
  *
  * @author liujjy
  * @date 2017/12/25 14:08
  */

object DBProperties {
  private  val pro = PropertiUtils.init("db.properties")

  var dbMap = new util.HashMap[String,String]()
  //home_data2 数据库
  val USER = pro.getProperty("user")
  val PASSWORD = pro.getProperty("password")
  val JDBC_URL = pro.getProperty("jdbc_url")


  //dtvs 数据库
  val USER_DTVS = pro.getProperty("user_dtvs")
  val PASSWORD_DTVS = pro.getProperty("password_dtvs")
  val JDBC_URL_DTVS = pro.getProperty("jdbc_url_dtvs")

  dbMap.put("USER_DTVS",USER_DTVS)
  dbMap.put("PASSWORD_DTVS",PASSWORD_DTVS)
  dbMap.put("JDBC_URL_DTVS",JDBC_URL_DTVS)

  // iusm 数据库
  val USER_IUSM = pro.getProperty("user_iusm")
  val PASSWORD_IUSM = pro.getProperty("password_iusm")
  val JDBC_URL_IUSM = pro.getProperty("jdbc_url_iusm")

  dbMap.put("USER_IUSM",USER_IUSM)
  dbMap.put("PASSWORD_IUSM",PASSWORD_IUSM)
  dbMap.put("JDBC_URL_IUSM",JDBC_URL_IUSM)

  // icore 数据库
  val USER_ICORE = pro.getProperty("user_icore")
  val PASSWORD_ICORE = pro.getProperty("password_icore")
  val JDBC_URL_ICORE = pro.getProperty("jdbc_url_icore")

  // homed_education 数据库
  val USER_EDU = pro.getProperty("user_edu")
  val PASSWORD_EDU = pro.getProperty("password_edu")
  val JDBC_URL_EDU = pro.getProperty("jdbc_url_edu")

  //homed_iacs
  val USER_IACS = pro.getProperty("user_iacs")
  val PASSWORD_IACS = pro.getProperty("password_iacs")
  val JDBC_URL_IACS = pro.getProperty("jdbc_url_iacs")

  dbMap.put("USER_IACS",USER_IACS)
  dbMap.put("PASSWORD_IACS",PASSWORD_IACS)
  dbMap.put("JDBC_URL_IACS",JDBC_URL_IACS)


  //homed_ilog
  val USER_ILOG = pro.getProperty("user_ilog")
  val PASSWORD_ILOG = pro.getProperty("password_ilog")
  val JDBC_URL_ILOG= pro.getProperty("jdbc_url_ilog")

  val JDBC_URL_TOPWAY=pro.getProperty("JDBC_URL_TOPWAY_BUSINESS")
  val USER_TOPWAY=pro.getProperty("USER_TOPWAY_BUSINESS")
  val PASSWORD_TOPWAY=pro.getProperty("PASSWORD_TOPWAY_BUSINESS")

  //homed_ocn
  val OCN_USER    =pro.getProperty("ocn_user")
  val OCN_PASSWORD=pro.getProperty("ocn_password")
  val OCN_JDBC_URL=pro.getProperty("ocn_jdbc_url")

  //homed_spider2
  val SPIDER2_USER=pro.getProperty("spider2_user")
  val SPIDER2_PASSWORD=pro.getProperty("spider2_password")
  val SPIDER2_JDBC_URL=pro.getProperty("spider2_jdbc_url")

  //homed_rank
  val USER_RANK=pro.getProperty("user_rank")
  val PASSWORD_RANK=pro.getProperty("password_rank")
  val JDBC_URL_RANK=pro.getProperty("jdbc_url_rank")

  //homed_user_profile
  val USER_PROFILE=pro.getProperty("user_profile")
  val PASSWORD_PROFILE=pro.getProperty("password_profile")
  val JDBC_URL_PROFILE=pro.getProperty("jdbc_url_profile")

  //bigdata_new_profile
  /*val USER_BIGDATA_NEW_PROFILE=pro.getProperty("user_bigdata_new")
  val PASSWORD_BIGDATA_NEW_PROFILE=pro.getProperty("password__bigdata_new")
  val JDBC_URL_BIGDATA_NEW_PROFILE=pro.getProperty("jdbc_url__bigdata_new")*/

  //###ad_interface_db @20190620广告系统接口数据库配置
  val USER_AD_INTERFACE=pro.getProperty("user_ad_interface")
  val PASSWORD_AD_INTERFACE=pro.getProperty("password_ad_interface")
  val JDBS_URL_AD_INTERFACE=pro.getProperty("jdbs_url_ad_interface")
}
