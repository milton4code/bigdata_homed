package cn.ipanel.utils

import java.util.Properties

/**
  *
  * 配置文件工具类
  *
  * @author liujjy
  * @date 2017/12/25 13:44
  */

object PropertiUtils {
  private val pro = new Properties()

  /**
    * 初始化配置文件
    * @param propertyFileName 配置文件名称
    * @return 配置文件对象
    */
  def init(propertyFileName: String): Properties = {
    try {
      val inputStream = PropertiUtils.getClass.getClassLoader.getResourceAsStream(propertyFileName)
      pro.load(inputStream)
      pro
    } catch {
      case ex: Exception =>
        throw new RuntimeException("加载配置异常", ex)
    }
  }

  /**
    * 获得mysql 连接配置
    * @return
    */
  def getMysqlProp(pro:Properties) = {
    try{
      val prop = new Properties()
      prop.put("user", pro.getProperty("mysql.user"))
      prop.put("password", pro.getProperty("mysql.pwd"))
      prop.put("driver",pro.getProperty("mysql.driver")) //表示驱动程序是com.mysql.jdbc.Driver
      prop
    }catch {
      case ex:Exception =>
        throw new RuntimeException("加载配置异常", ex)
    }
  }

}
