package cn.ipanel.ocn.common

import cn.ipanel.utils.PropertiUtils

/**
  * 数据库属性
  *
  * @author liujjy
  * @date 2017/12/25 14:08
  */

private [ocn] object Properties {
  private  val pro = PropertiUtils.init("db.properties")
  val OCN_USER    =pro.getProperty("ocn_user")
  val OCN_PASSWORD=pro.getProperty("ocn_password")
  val OCN_JDBC_URL=pro.getProperty("ocn_jdbc_url")
  val POST_URL = pro.getProperty("post_url")
}
