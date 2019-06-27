package cn.ipanel.etl

import cn.ipanel.common.CluserProperties

/**
  *
  * created by liujjy at 2018/03/28 10:59
  */

object LogConstant {
  val userId = "DA"
  val deviceType = "DeviceType"
  val deviceId = "DeviceID"
  val ProgramMethod = "ProgramMethod"
  val programID = "ProgramID"
  val playS = "PlayS"
  val search = "keyword"
  //返回的搜索的节目ID列表
  val filter = "SearchId"
  //下载
  val assetDownload = "nProgramid"
  //收藏
  val setFavoriteSuccess = "FavoriteId"
  //取消收藏
  val cancelFavoriteSuccess = "FavoriteId"
  //预约成功
  val eventSetOrderSuccess = "EventId"
  val eventCancelOrderSuccess = "EventId"
  //分享成功
  val shareSuccess = ""
  //点赞
  val programPraise = "ProgramID"
  //成功登陆的用户
  val loginSuccess = ""
  //用户登出成功
  val logoutSuccess = ""
  //用户计费成功
  val money = "Money"

  //开始
  val SUCCESS = true
  //结束
  val FINISH = false

  val videoSuccess = "StatisticsVideoPlaySuccess"
  val videoFinished = "StatisticsVideoPlayFinished"
  val videoFailed = "StatisticsVideoPlayFailed"
  val videoBreak = "VideoPlayBreak"
  val programEnter = "ProgramEnter"
  val programExit = "ProgramExit"

  //hdfs上的arate文件路径
  val HDFS_ARATE_LOG = CluserProperties.HDFS_ARATE_LOG
  //ilogslave日志存放路径
  val LOG_PATH = CluserProperties.LOG_PATH
  //homed2.0 推流日志存放路径
  val LOGVSS_PATH = CluserProperties.LOGVSS_PATH
  //  nginx上文件路径
  val NGINX_JSON_LOG_PATH = CluserProperties.NGINX_JSON_LOG_PATH

  //iusm 日志路径
  val IUSM_LOG_PATH = CluserProperties.IUSM_LOG_PATH
  //iacs 日志路径
  val IACS_LOG_PATH = CluserProperties.IACS_LOG_PATH

  //点播用户上报还是run日志
  val DEMAND_REPORT = CluserProperties.DEMAND_REPORT
  val LOOK_REPORT = CluserProperties.LOOK_REPORT
  val LIVE_REPORT = CluserProperties.LIVE_REPORT
  val TIMESHIFT_REPORT = CluserProperties.TIMESHIFT_REPORT

  //用户区域版本控制
  val REGIONVERSION = CluserProperties.REGION_VERSION

  //终端默认配置
  val TERMINAL = CluserProperties.TERMINAL

  //机型默认配置
  val PHONEMODEL = CluserProperties.PHONEMODEL
  val APPVERSION = CluserProperties.APPVERSION
  val HARDVERSION = CluserProperties.HARDVERSION
  val SOFTVERSION = CluserProperties.SOFTVERSION
  val MAVERSION = CluserProperties.MAVERSION
  //终端版本
  val STATISTICS_TYPE = CluserProperties.STATISTICS_TYPE
}
