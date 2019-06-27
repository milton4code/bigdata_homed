package cn.ipanel.common

/**
  * 常量
  *
  * @author liujjy  
  * @date 2017/12/26 15:28
  */

object Constant {
  /** 日志 key value 对分隔符 */
  val K_V_SPILT = "&"

  /**run日志分割符*/
  val SPLIT = ","
  /** 文件路径/切割符 */
  val FILE_PATH_SPLIT = "/"
  /** redis 字符串分隔符 */
  val REDIS_SPILT ="|"

  /** 根栏目id=0*/
  val COLUMN_ROOT = "0"

  val BLOCK_SIZE = 64
  val HIVE_DB = "bigdata"
  /**省网*/
  val PROVINCE = "province"
  /**市网*/
  val CITY = "city"
  /**空区域码*/
  val EMPTY_CODE = "000000"

  /** 用户行为上报心跳间隔*/
  val SYSTEM_HEARTBEAT_DURATION = 60

  /** 未知类型 */
  val UNKOWN="unkown"

  /** 默认homed  版本*/
  val OLDER_HOMED_OVERION = 1.4

  //-------------------------------------------
  /** 湖南区域码 */
  val JILIN_CODE = "220000"
  /** 宁夏区域码*/
  val NX_CODE= "640000"
  /** 湖南区域码 */
  val HUNAN_CODE = "430000"
  /** 芜湖区域码 */
  val WUHU_CODE = "340200"
  /** 番禺区域码 (实际是广州的区域码)*/
  val PANYU_CODE = "440100"
  //--------------------------------------

  /** 区域部署范围 0 表示全国, 1表示 省网*/
  val REGION_VERSION_ALL = "0"
  val REGION_VERSION_PROVICE = "1"


}
