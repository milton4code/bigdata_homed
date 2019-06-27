package cn.ipanel.common

import cn.ipanel.utils.PropertiUtils

/**
  * 集群配置属性
  *
  * @author liujjy  
  * @date 2017/12/26 14:47
  */

object CluserProperties {

  private val pro = PropertiUtils.init("cluster.properties")

  val SPARK_MASTER_URL = pro.getProperty("master_url")

  // hbase
  val ZOOKEEPER_URL = pro.getProperty("zookeeper.url")
  val HBASE_CLIENT_PORT = pro.getProperty("hbase.zookeeper.property.clientPort")
  val HBASE_ZK_PARENT = pro.getProperty("hbase.zookeeper.znode.parent")

  // phoenix
  val PHOENIX_ZKURL = pro.getProperty("phonnix.zkurl")

  val REGION_CODE = pro.getProperty("region_code")

  //kafka 集群信息
  val KAFKA_BROKERS = pro.getProperty("kafka.brokers")

  //
  val CHANNEL_PUT_URL = pro.getProperty("channel_rank_put_url")
  val USER_PUT_URL = pro.getProperty("user_put_url")
  val HDFS_ARATE_LOG = pro.get("HDFS_ARATE_LOG")
  val LOG_PATH = pro.get("ILOGSLAVE_LOG_PATH")
  val NGINX_JSON_LOG_PATH = pro.get("NGINX_JSON_LOG_PATH")
  val IUSM_LOG_PATH = pro.get("IUSM_LOG_PATH")
  val IACS_LOG_PATH = pro.get("IACS_LOG_PATH")
  val LOGVSS_PATH = pro.get("LOGVSS_LOG_PATH")


  val DEMAND_REPORT = pro.getProperty("demand_report")
  val LOOK_REPORT = pro.getProperty("look_report")
  val LIVE_REPORT = pro.getProperty("live_report")
  val TIMESHIFT_REPORT = pro.getProperty("timeshift_report")

  val REGION_VERSION = pro.getProperty("region_version")
  val TERMINAL = pro.getProperty("terminal")

  /** 数据依赖源 */
  val LOG_SOURCE = pro.getProperty("log_source")

  /** 统计类型 */
  val STATISTICS_TYPE = pro.getProperty("statistics_type").toInt

  val PHONEMODEL = pro.getProperty("f_phone_model")
  val APPVERSION = pro.getProperty("f_app_version")
  val HARDVERSION = pro.getProperty("f_hard_version")
  val SOFTVERSION = pro.getProperty("f_soft_version")
  val MAVERSION = pro.getProperty("f_manufacturer")
  /** 心跳间隔 */
  val HEART_BEAT = pro.getProperty("heart_beat").toInt
}
