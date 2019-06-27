package cn.ipanel.ocn.utils

import cn.ipanel.ocn.etl.OcnConstant
import cn.ipanel.ocn.etl.OcnLogParser.Log
import cn.ipanel.utils.DateUtils

import scala.collection.mutable.{HashMap => scalaHashMap}
/**
  * LogUtils<br> 
  * ${DESCRIPTION}
  *
  * @author liujjy
  * @create 2018/5/30
  * @since 1.0.0
  */
private [ocn] object OcnLogUtils {

  /**
    * 设备ID映射为设备类型
    * 终端1stb,2 mobile,3 pad, 4pc,0其他
    *
    * @param deviceId 　设备ID
    * @return 设备类型
    */
  def deviceIdMapToDeviceType(deviceId: String): String = {
    var deviceType = OcnConstant.UNKNOWN_TYPE
    try {
      val device = deviceId.toLong
      if (device >= 2000000000l && device < 3000000000l) {
        deviceType = "2"
      } else if (device >= 3000000000l && device < 4000000000l) {
        deviceType = "4"
      } else if (device >= 1800000000l && device < 1900000000l) {
        deviceType = "3"
      } else if (device >= 1000000000l && device < 1200000000l) {
        deviceType = "1"
      } else if (device >= 1400000000l && device < 1600000000l) {
        deviceType = "1"
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    deviceType
  }

  /**
    * 转换
    * 时间戳转换为 yyyy-MM-dd HH:mm:ss格式
    * 将设备ID转换为对应的设备类型
    *
    * @param data 包含Log类型日志
    */
  def tranform(data: String): Log = {
    var log = Log(params = new scalaHashMap[String, String]())
    val datas = data.split(OcnConstant.SPLIT)
    if (datas.length == 5) {
      val report_time = DateUtils.timestampFormat(datas(1).toLong, DateUtils.YYYY_MM_DD_HHMMSS)
      val deviceType = OcnLogUtils.deviceIdMapToDeviceType(datas(4))
      try {
        log = Log(datas(0), report_time, datas(2), datas(3), datas(4), deviceType, new scalaHashMap[String, String]()).copy()
      } catch {
        case ex: Exception =>  ex.printStackTrace()
      }
    }
    log
  }

}
