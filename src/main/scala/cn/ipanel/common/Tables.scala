package cn.ipanel.common

/**
  * 存放表名
  */
object Tables {
  val meizi_statisc_report = "t_meizi_statisc_report"
  val t_meizi_lookback_report = "t_meizi_lookback_report"

  //在库媒资
  val meizi_content_chart = "t_meizi_lookback_content_chart"
  //回看
  val meizi_cp_content_chart = "t_meizi_cp_content_chart"
  //点播
  //终端点播
  val meizi_statistics_back = "t_meizi_statistics_back"

  //营收报表
  val t_revenue_report = "t_revenue_report"
  //栏目报表
  val mysql_user_column_summary = "t_user_column_summary"
  val T_USER_COLUMN_SUMMARY = "t_user_column_summary"
  //栏目报表数据通过phoenix存入到hbase中
  val hbase_user_column = "t_user_column"
  //栏目报表数据存入
  val mysql_user_column = "t_user_column"
  //搜索报表数据存入
  val mysql_user_search_keyword = "t_user_search_keyword"
  //筛选报表统计
  val mysql_user_filter = "t_user_filter"
  //家庭开户报表数据存入
  val mysql_home_open = "t_home_open"
  //媒资统计(频道)按终端、时间段、区域
  val t_meizi_statistics_terminal_timerange = "t_meizi_statistics_terminal_timerange"
  //媒资统计(频道)按终端、时间段、频道类型
  val t_meizi_statistics_terminal_channel = "t_meizi_statistics_terminal_channel"
  //媒资统计(频道)频道收视详情
  val t_meizi_statistics_channel_detail = "t_meizi_statistics_channel_detail"
  //媒资统计(频道)基础表
  val t_meizi_statistics_channel_base = "t_meizi_statistics_channel_base"
  //媒资库存
  val t_media_repertory = "t_media_repertory"
  //个人开户报表
  val t_personal_open_account = "t_personal_open_account"
  //推荐成功率
  val t_recommend_success_rate = "t_recommend_success_rate"
  //整体营业收入（按订购内容聚合）
  val t_business_operation_revenue_ordercontent = "t_business_operation_revenue_ordercontent"
  //整体营业收入（按区域聚合）
  val t_business_operation_revenue_region = "t_business_operation_revenue_region"
  //整体营业收入（按支付方式）
  val t_business_operation_revenue_paytype = "t_business_operation_revenue_paytype"
  //4.整体营业收入(套餐销量)
  val t_business_operation_revenue_package_sales = "t_business_operation_revenue_package_sales"
  //整体营业收入(单片销量)
  val t_business_operation_revenue_danpian_sales = "t_business_operation_revenue_danpian_sales"
  //整体营业收入(订购活跃度)
  val t_business_operation_revenue_order_liveness = "t_business_operation_revenue_order_liveness"
  //cp/sp营业收入（用于营业占比和销售变化）
  val t_business_cp_sp_incoming = "t_business_cp_sp_incoming"
  //cp/sp营业收入（按订购内容）
  val t_business_cp_sp_ordercontent = "t_business_cp_sp_ordercontent"
  //业务运营cp/sp套餐销量排名
  val t_business_cp_sp_package_sales = "t_business_cp_sp_package_sales"
  //业务运营cp/sp单片销量排名
  val t_business_cp_sp_danpian_sales = "t_business_cp_sp_danpian_sales"
  //业务营收套餐销量
  val t_business_operation_package_sales = "t_business_operation_package_sales"
  //业务运营基础表
  val t_business_operation = "t_business_operation"
  // 回看
  val t_look_back_statistics_report = "t_look_back_statistics_report"
  val t_lookback_online_report = "t_look_user_by_halfhour"
  //点播
  val t_demand_user_by_halfhour = "t_demand_user_by_halfhour"
  val t_demand_watch_statistics_report = "t_demand_watch_statistics_report"
  //每个用户观看点播的次数 时间
  val t_active_demand_user = "t_active_demand_user"
  //广告
  val t_ad_report = "t_ad_report"
  //在线率
  val t_online_rate = "t_online_rate"
  //按天在线率
  val t_day_online_rate = "t_online_rate_day"
  /** 终端在线用户表 按秒保存 */
  //val T_CHANNEL_ONLINE_USER_SECOND= "t_channel_online_user_second"
  /** 终端在线用户表 按分钟保存 */
  //val T_CHANNEL_ONLINE_USER_MINUTE= "t_channel_online_user_minute"
  /** 终端 设备在线用户 */
  val T_TERMINAL_DEVICE_ONLINE = "t_terminal_device_online"
  /** 频道在线页面列表数据,按秒保存 */
  val T_CHANNEL_PAGE_BY_SECOND = "t_channel_page_by_second"
  /** 解析后的用户行为日志数据存放表 */
  val ORC_USER_REPORT = "orc_user_report"
  /** 广告 */
  val T_AD_REPORT = "t_ad_report"

  /** added by lizhy@20180927 */

  /** homed终端设备表*/
  val T_DEVICE_INFO = "device_info"
  /** homed设备地址表*/
  val T_ADDRESS_DEVICE = "address_device"
  /** homed地址信息表*/
  val T_ADDRESS_INFO = "address_info"
  /** 节目安排表 */
  val PROGRAM_SCHEDULE = "homed_eit_schedule_history"
  /** 频道存储表 */
  val CHANNEL_STORE = "channel_store"
  /** 访问概况表 */
  val VISIT_OVERVIEW = "t_visit_overview_count"
  /** redis用户在线状态表 */
  val USER_ONLINE_STATUS = "t_user_online_status"
  /** homed websocket 在线用户状态表mysql */
  val USER_ONLINE_WEBSOCKET = "t_user_online_statistics"
  //====================================================================
  /*********************** added by lizhy@20180927 ******************/
  /** 直播统计表，分为半小时、日、周、月、季度、年、7天内、30天内、1年内表*/
  val T_CHANNEL_LIVE_PROGRAM_BY_FIFTEEN = "t_live_channel_program_by_fifteen"
  val T_CHANNEL_LIVE_PROGRAM_BY_DAY = "t_live_channel_program_by_day"
  val T_CHANNEL_LIVE_PROGRAM_FIVEMINUTE = "t_live_channel_program_by_fiveminute"
  val T_CHANNEL_LIVE_HALFHOUR = "t_live_channel_by_halfhour"
  val T_CHANNEL_LIVE_DAY = "t_live_channel_by_day"
  val T_CHANNEL_LIVE_WEEK  = "t_live_channel_by_week"
  val T_CHANNEL_LIVE_MONTH = "t_live_channel_by_month"
  val T_CHANNEL_LIVE_QUARTER = "t_live_channel_by_quarter"
  val T_CHANNEL_LIVE_YEAR = "t_live_channel_by_year"
  val T_CHANNEL_LIVE_HIS = "t_live_channel_by_history"
  /**直播用户集合表-hive*/
  val T_USER_ARRAY_BY_DAY = "t_user_array_by_day"
  /**节目安排表*/
  val T_PROGRAM_SCHEDULE = "homed_eit_schedule_history"
  /**频道存储表*/
  val T_CHANNEL_STORE = "channel_store"
  /**访问概况表*/
  val t_VISIT_OVERVIEW = "t_visit_overview_count"
/*  /**redis用户在线状态表*/
  val T_USER_ONLINE_STATUS = "t_user_online_status"*/
  /**homed websocket 在线用户状态表mysql*/
  val T_USER_ONLINE_WEBSOCKET= "t_user_online_statistics"
  /*********************** 实时部分开始 by lizhy******************/
  /** 直播节目安排表*/
  val T_HOMED_EIT_SCHEDULE = "homed_eit_schedule"
  /** homed用户表*/
  val T_ACCOUNT_INFO = "account_info"
  /** homed区域表*/
  val T_AREA = "area"
  /** homed城市表*/
  val T_CITY = "city"
  /** homed省表*/
  val T_PROVINCE = "province"
  /** 终端类型描述表*/
  val T_TERMINAL = "t_terminal"
  /** 直播频道实时表*/
  val T_CHNN_LIVE_REALTIME = "t_channel_live_realtime"
  /** 在线用户数统计表 */
  val T_USER_COUNT_REALTIME = "t_user_count_realtime"
  /** 区域终端统计业务类型配置表 */
  val T_REGION_TERMINAL_SERVICE_TYPE = "t_region_terminal_service_type"
  /** 实时频道类型播放时长统计表*/
  val T_CHANNEL_TYPE_PLAYTIME_REALTIME= "t_channel_type_playtime_realtime"
  /** 实时节目点播统计表*/
  val T_PROGRAM_DEMAND_REALTIME = "t_program_demand_realtime"
  /** 实时点播内容类型统计表*/
  val T_DEMAND_CONTENT_TYPE_REALTIME = "t_demand_content_type_realtime"
  /** 实时节目回看统计*/
  val T_PROGRAM_LOOKBACK_REALTIME = "t_program_lookback_realtime"
  /** 实时节目回看内容分类表*/
  val T_PROGRAM_LOOKBACK_TYPE_REALTIME = "t_program_lookback_type_realtime"
  /** 实时节目点播历史表*/
  val T_PROGRAM_DEMAND_HIS_REALTIME = "t_program_demand_history_realtime"
  /** 实时时间节点表*/
  val T_TIMENODE_REALTIME = "t_time_node_realtime"
  /** 用户实时状态表*/
  val T_RUNLOG_USER_STATUS_REALTIME = "t_runlog_user_status_realtime"
  val T_USERLOG_USER_STATUS_REALTIME = "t_userlog_user_status_realtime"
  /** 用户区域表*/
  val T_USER_REGION = "t_user_region"
  /*********************** 实时部分结束 ******************/

  /** 开机时长 半小时，天，自然周，自然月，季度，自然年，7天内、30天内、一年内*/
  val T_OPEN_TIME_HALFHOUR = "t_opentime_by_halfhour"
  val T_OPEN_TIME_DAY = "t_opentime_by_day"
  val T_OPEN_TIME_WEEK = "t_opentime_by_week"
  val T_OPEN_TIME_MONTH = "t_opentime_by_month"
  val T_OPEN_TIME_QUARTER = "t_opentime_by_quarter"
  val T_OPEN_TIME_YEAR = "t_opentime_by_year"
  val T_OPEN_TIME_HISTORY = "t_opentime_history"
  /** 运营汇总、业务访问情况统计表 半小时，天，自然周，自然月，季度，自然年，7天内、30天内、一年内*/
  val T_BUS_ARRAY_DAY = "t_business_array_by_day"
  val T_BUS_VISIT_HALFHOUR = "t_business_visit_by_halfhour"
  val T_BUS_VISIT_DAY = "t_business_visit_by_day"
  val T_BUS_VISIT_WEEK = "t_business_visit_by_week"
  val T_BUS_VISIT_MONTH = "t_business_visit_by_month"
  val T_BUS_VISIT_QUARTER = "t_business_visit_by_quarter"
  val T_BUS_VISIT_YEAR = "t_business_visit_by_year"
  val T_BUS_VISIT_HISTORY = "t_business_visit_by_history"
  val T_SERVICE_VISIT_USER_TOP_RANK = "t_service_visit_user_top_rank"
  val T_SERVICE_VISIT_USER = "t_service_visit_users" //业务访问全量用户表@20190410
  val T_INACTIVE_USERS_BY_SERVICE = "t_inactive_users_by_service" //各业务不活跃用户报表
  /** 内容提供商相关统计表 added@20190510 by lizhy*/
  val T_DEMAND_CP_SP_RANK_BY_HOUR = "t_demand_cp_sp_rank_by_hour"
  val T_DEMAND_CP_SP_RANK = "t_demand_cp_sp_rank"
  val T_DEMAND_CP_SP_USER_PLAY = "t_demand_cp_sp_user_play"
  val T_DEMAND_CP_SP_VIDEO_PLAY = "t_demand_cp_sp_video_play"
  val T_DEMAND_CP_SP_USER_TYPE = "t_demand_cp_sp_user_type"

  //=====================================================================
  /** hive表 */
  //run日志中视频播放数据
  val ORC_VIDEO_PLAY_TMP = "orc_video_play_tmp"
  val ORC_VIDEO_PLAY = "orc_video_play"
  val ORC_VIDEO_PLAY_RECOMMEND = "orc_video_play_recommend"
  //run日志中用户行为数据
  val ORC_USER_BEHAVIOR = "orc_user_behavior"
  //nginx日志数据
  val ORC_NGINX_LOG = "orc_nginx_log"
  //run日志播放相关的没有合并开始\结束时间数据
  val ORC_USER_VIDEO_PLAY = "orc_user_video_play"
  //=========================================================================
  /** 日新增绑卡用户 */
  val Dalian_Bind_Login = "t_bind_login"
  /** 日新增用户 */
  val Dalian_New_Open = "t_new_open"
  /** 活跃用户 */
  val Dalian_User_Active = "t_all_active_users"
  /** 图为点击 */
  val Dalian_User_Hit = "t_user_hit"

  //在库媒资
  val t_media_video_content_sub = "t_media_video_content_sub"
  val t_media_event_content_sub = "t_media_event_content_sub"
  val t_cp_up_under_count = "t_cp_up_under_count"
  val t_media_status = "t_media_status"
  //业务排行
  val t_click_upload_count = "t_click_upload_count"
  val orc_sence_upload = "orc_sence_upload"
  val t_recommend_column_by_day = "t_recommend_column_by_day"
  val t_recommend_column_by_month = "t_recommend_column_by_month"
  val t_recommend_column_by_week = "t_recommend_column_by_week"
  val t_recommend_column_by_year = "t_recommend_column_by_year"
  val t_recommend_column_by_quarter = "t_recommend_column_by_quarter"
  val t_recommend_column_by_history = "t_recommend_column_by_history"

  //营收有关的
  val t_business_basic = "t_business_basic"
  //mysql基础数据
  val t_business_basic_by_day = "t_business_basic_by_day"
  //hive基础数据
  val t_business_user_all_count = "t_business_user_all_count"
  //退订有关的 退订用户数和退订数
  val t_unsubscribe_by_day = "t_unsubscribe_by_day"
  val t_unsubscribe_by_month = "t_unsubscribe_by_month"
  val t_unsubscribe_by_year = "t_unsubscribe_by_year"
  val t_unsubscribe_by_quarter = "t_unsubscribe_by_quarter"
  val t_unsubscribe_by_week = "t_unsubscribe_by_week"
  val t_unsubscribe_by_history = "t_unsubscribe_by_history"


  //点播节目
  val t_demand_report_by_day = "t_demand_report_by_day"
  val t_demand_report_by_month = "t_demand_report_by_month"
  val t_demand_report_by_year = "t_demand_report_by_year"
  val t_demand_report_by_week = "t_demand_report_by_week"
  val t_demand_report_by_history = "t_demand_report_by_history"
  val t_demand_user_by_day = "t_demand_user_by_day"
  val t_demand_user_by_month = "t_demand_user_by_month"
  val t_demand_user_by_year = "t_demand_user_by_year"
  val t_demand_user_by_week = "t_demand_user_by_week"
  val t_demand_user_by_history = "t_demand_user_by_history"
 //点播剧集
  val t_demand_series_report_by_day = "t_demand_series_report_by_day"
  val t_demand_series_report_by_month = "t_demand_series_report_by_month"
  val t_demand_series_report_by_week = "t_demand_series_report_by_week"
  val t_demand_series_report_by_history = "t_demand_series_report_by_history"


  //回看
  val t_lookback_report_by_day = "t_lookback_report_by_day"
  val t_lookback_report_by_month = "t_lookback_report_by_month"
  val t_lookback_report_by_year = "t_lookback_report_by_year"
  val t_lookback_report_by_week = "t_lookback_report_by_week"
  val t_lookback_report_by_history = "t_lookback_report_by_history"

  val t_look_user_by_day = "t_look_user_by_day"
  val t_look_user_by_month = "t_look_user_by_month"
  val t_look_user_by_week = "t_look_user_by_week"
  val t_look_user_by_year = "t_look_user_by_year"
  val t_look_user_by_history = "t_look_user_by_history"


  //点播栏目
  val t_column_user_count = "t_column_user_count"
  val t_column_demand_count = "t_column_demand_count"

  //点播套餐
  val orc_user_package = "orc_user_package"
  val t_package_user_count = "t_package_user_count"
  val t_package_demand_count = "t_package_demand_count"

  //开机报表
  val t_manufacturer_report_by_day ="t_manufacturer_report_by_day"
  val t_manufacturer_report_by_week ="t_manufacturer_report_by_week"
  val t_manufacturer_report_by_month="t_manufacturer_report_by_month"
  val t_manufacturer_report_by_history="t_manufacturer_report_by_history"

  val t_appversion_report_by_day ="t_appversion_report_by_day"
  val t_appversion_report_by_month ="t_appversion_report_by_month"
  val t_appversion_report_by_week ="t_appversion_report_by_week"
  val t_appversion_report_by_year ="t_appversion_report_by_year"
  val t_appversion_report_by_history ="t_appversion_report_by_history"
  val t_appversion_ca_details_by_day ="t_appversion_ca_details_by_day"

  val t_appversion_ca_details ="t_appversion_ca_details"

  //iacs日志
  val ORC_IACS = "orc_iacs"

  //点播栏目的节目详情
  val t_demand_column_by_halfhour = "t_demand_column_by_halfhour"
  val t_demand_column_by_day="t_demand_column_by_day"
  val t_demand_column_by_month="t_demand_column_by_month"
  val t_demand_column_by_week="t_demand_column_by_week"
  val t_demand_column_by_year="t_demand_column_by_year"
  val t_demand_column_by_history="t_demand_column_by_history"

  //套餐下节目id
  val t_package_demand_video_by_day = "t_package_demand_video_by_day"
  val t_package_demand_video_by_week = "t_package_demand_video_by_week"
  val t_package_demand_video_by_month = "t_package_demand_video_by_month"
  val t_package_demand_video_by_year = "t_package_demand_video_by_year"
  val t_package_demand_video_by_history ="t_package_demand_video_by_history"

  //用户播放详情
  val F_COLUMN_VIDEO_REPORT = "t_column_video_report"

  //用户登出离线数据
  val T_USER_ONLINE_HISTORY = "t_user_online_history"

  /**区域信息对应关系*/
  val ORC_AREA = "orc_area"
  /** 每天登陆过的用户数据 ,去重*/
  val ORC_LOGIN_USER = "orc_login_user"
  /** 历史token数据去重*/
  val ACCOUNT_TOKEN = "account_token"
  /** 硬件版本号*/
  val APP_VERSION = "app_version"


  //-----------用户统计模块--------------
  /** 用户总量统计*/
  val T_USER_SUMMARY = "t_user_summary"
  /** 开机半小时明细数据表 */
  val T_ONLINE_DEV_HALFHOUR = "t_online_dev_halfhour"
  /** 开机用户在线时长及开机用户数 */
  val T_ONLINE_USER_TIME = "t_online_user_time"

}
