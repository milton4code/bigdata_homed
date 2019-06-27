package cn.ipanel.homed.realtime

object GatherConstant {

  val userId = "DA"
  val deviceType = "DeviceType"
  val deviceId = "DeviceID"
  val ProgramMethod = "ProgramMethod"
  val programID = "ProgramID"
  val playS = "PlayS"
  val videoSuccessNew = "StatisticsVideoPlaySuccess"
  val videoFinishedNew = "StatisticsVideoPlayFinished"
  val videoFailedNew = "StatisticsVideoPlayFailed"
  val videoSuccess = "VideoPlayStartSuccess"
  val videoFinished = "VideoPlayFinish"
  val videoFailed = "VideoPlayFailed"
  val videoBreak = "VideoPlayBreak"
  val programEnter = "ProgramEnter"
  val programExit = "ProgramExit"

  //--------------业务类型码-------------------------
  // guest 游客
  val GUEST = "guest"
  val GUEST2 = "游客"

  //直播101 时移102 回看103 点播104
  /** 心跳 */
  val USER_LOG_HEARTBREATH ="0701"
  /** 直播 */
  val USER_REPORT_LIVE ="101"
  /** 时移*/
  val USER_REPORT_TIME_SHIFT ="102"
  /** 回看*/
  val USER_REPORT_LOOK_BACK = "103"
  /** 点播*/
  val USER_REPORT_DEMAND = "104"
  /**栏目131*/
  val USER_REPORT_PROGRAM_DETAIL="131"

  /** 直播 */
  val LIVE ="live"
  /** 时移*/
  val TIME_SHIFT ="timeshift"
  /** 一键时移*/
  val ONE_KEY_TIMESHIFT ="one-key-timeshift"
  /** 回看*/
  val LOOK_BACK = "lookback"
  /** 点播*/
  val DEMAND= "demand"

  /** 直播 */
  val RUN_LOG_LIVE_NEW ="live"
  /** 时移*/
  val RUN_LOG_TIME_SHIFT_NEW ="timeshift"
  /** 一键时移*/
  val RUN_LOG_ONE_KEY_TIMESHIFT_NEW ="one-key-timeshift"
  /** 回看*/
  val RUN_LOG_LOOK_BACK_NEW = "lookback"
  /** 点播*/
  val RUN_LOG_DEMAND_NEW= "demand"
  // 推荐
  val RUN_LOG_RECOMMEND_TYPE = "recommend"
  /** 未知频道 */
  val UNKNOWN_CHANNEL="unknown"

  val RUN_LOG_LOOK_BACK = "tr"
  val RUN_LOG_TIME_SHIFT =  "ts"
  val RUN_LOG_ONE_KEY_TIMESHIFT = "kts"
  val RUN_LOG_DEMAND =  "vod"
  val RUN_LOG_LIVE =  "live"
  //-----心跳场景	参数S取值-----
  /** 直播	1 */
  val BREAK_SCEN_LIVE = "1"
  /** 时移	2 */
  val BREAK_SCEN_TIEMSFT = "2"
  /** 回看	3 */
  val BREAK_SCEN_LOOKBK = "3"
  /** 点播	4 */
  val BREAK_SCEN_DEMAND = "4"
  /** 应用	5 */
  val BREAK_SCEN_APPL = "5"
  /** 栏目	6  */
  val BREAK_SCEN_PROG = "6"

  val RUN_LOG_LIVE_PRO_KEY = "ProgramMethod live"
  /** 各业务场景采取的日志方式 added by lizhy@20190609*/
  val RUN_LOG_GATHER_TYPE = List(RUN_LOG_LIVE,RUN_LOG_DEMAND,RUN_LOG_LOOK_BACK,RUN_LOG_TIME_SHIFT)
  val USER_LOG_GATHER_TYPE = List(USER_REPORT_LIVE)

}
