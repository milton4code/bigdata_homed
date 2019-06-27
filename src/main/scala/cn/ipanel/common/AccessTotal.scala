package cn.ipanel.common

/**
  * 直播、回看、时移、点播 访问次数统计
  *
  * @param f_daytime     时间 yyyyMMdd
  * @param f_sevice_type 服务类型
  * @param f_terminal    终端类型
  * @param f_access_cnt  访问次数
  *                      //  * @param region     区域码
  */
case class AccessTotal(f_daytime: String, f_sevice_type: String, f_terminal: String, f_access_cnt: Int /*f_business: String  region: String*/)

/**
  * 直播、回看、时移、点播 用户统计个数统计
  *
  * @param f_daytime      时间 yyyyMMdd
  * @param f_service_type 服务类型
  * @param f_userid       用户ID
  * @param f_access_cnt   访问次数
  */
case class UserDetailStatistics(f_daytime: String, f_service_type: String, f_userid: String, f_access_cnt: Int)


/**
  * 推荐成功率统计
  *
  * @param f_date          时间 yyyyMMdd
  * @param f_rec_count     推荐总量
  * @param f_success_count 推荐成功数量
  */
case class RecommendSuccessRateRec(f_date: String, f_rec_count: Long, f_success_count: Long)

/**
  * 终端打开时间
  *
  * @param daytime    时间
  * @param terminal   终端类型
  * @param open_times 打开总时间
  */
case class TerminalOpenTime(daytime: String, terminal: String, open_times: Long)

/**
  * 终端打开用户
  *
  * @param daytime     时间
  * @param terminal    终端类型
  * @param open_userid 用户ID
  * @param service     服务类型
  * @param region      区域Id
  */
case class TerminalOpenUser(daytime: String, terminal: String, open_userid: String, service: String, region: String)

/**
  * 已解析的日志
  *
  * @param service   服务码1
  * @param service2  服务码2
  * @param startTime 开始时间戳
  * @param endTime   结束时间戳
  * @param date      时间 yyyyMMdd
  * @param userid    用户id
  * @param region    区域
  * @param terminal  终端类型
  */
case class ParsedLog(service: String, service2: String, startTime: Long, endTime: Long,
                     date: String, userid: String, region: String, terminal: String)

/**
  * 终端打开次数
  *
  * @param daytime  时间
  * @param terminal 终端类型
  * @param service  服务
  * @param region   区域
  * @param open_cnt 开机次数
  */
case class TerminalOpenCount(daytime: String, terminal: String, service: String, region: String, open_cnt: Int)


/**
  * 栏目收视详情
  *
  * @param daytime  日期
  * @param hour     小时值
  * @param userid   用户id
  * @param label_id 栏目id
  * @param uv       浏览量
  * @param terminal 终端
  */
case class ColumnDetail(daytime: String, hour: Int, userid: String, label_id: String, uv: Int, terminal: String)


/**
  * 整体营业收入
  *
  * @param f_order_date         日期yyyyMMdd
  * @param f_package_id         套餐id
  * @param f_package_name       套餐名
  * @param f_city_id            地市id
  * @param f_city_name          地市名
  * @param f_region_id          区域id
  * @param f_region_name        区域名
  * @param f_revenue_type       营收类型1:充值2:优惠券购买3:套餐订购
  * @param f_package_order_type 套餐订购方式 1:boss余额2:第三方支付
  * @param f_order_platform     订购发起平台 1:STB, 2:CA Card, 3:MOBILE 4:PAD, 5:PC
  * @param f_order_content      订购内容 1:直播，2:点播；3:回看，4:应用，5:时移，6:综合
  * @param f_order_price        订单金额(单位：分)
  */
case class OperationRevenue(f_order_date: String, f_package_id: Long, f_package_name: String, f_city_id: Long, f_city_name: String, f_region_id: Long, f_region_name: String
                            , f_revenue_type: Int, f_package_order_type: Int, f_order_platform: Int, f_order_content: Int, f_order_price: Int)

/**
  * cpsp收入明细
  *
  * @param f_order_date         日期yyyyMMdd
  * @param f_cp_sp              服务或内容提供商
  * @param f_city_id            地市id
  * @param f_city_name          地市名
  * @param f_region_id          区域id
  * @param f_region_name        区域名
  * @param f_payment_type       支付途径 1:余额付款 2:第三方支付
  * @param f_third_party_detail 第三方支付详情,支付途径为2时有效,1:银行卡，2:银联卡，3:微信,4:支付宝，5:苹果支付
  * @param f_price              订单金额（单位：分）
  */
case class CpSpIncomeDetail(f_order_date: String, f_cp_sp: String, f_city_id: Long, f_city_name: String, f_region_id: Long, f_region_name: String
                            , f_payment_type: Int, f_third_party_detail: Int, f_price: Int)

/**
  * 订购活跃度
  *
  * @param f_order_date        日期yyyyMMdd
  * @param f_cp_sp             内容或服务提供商
  * @param f_city_id           地市id
  * @param f_city_name         地市名
  * @param f_region_id         区域id
  * @param f_region_name       区域名
  * @param f_order_change_type 订购变化类型 1:新增订购 2:新增退订 3:新增到期 4:新增续订
  */
case class OrderLiveness(f_order_date: String, f_cp_sp: String, f_city_id: Long, f_city_name: String, f_region_id: Long, f_region_name: String, f_order_change_type: Int)

/**
  * 套餐订购量
  *
  * @param f_order_date   日期yyyyMMdd
  * @param f_city_id      地市id
  * @param f_city_name    地市名
  * @param f_region_id    区域id
  * @param f_region_name  区域名
  * @param f_package_type 套餐类型 1:单片 2:整包(套餐)
  * @param f_package_id   套餐id
  * @param f_package_name 套餐名
  */
case class PackageOrderQuantity(f_order_date: String, f_city_id: Long, f_city_name: String, f_region_id: Long, f_region_name: String
                                , f_package_type: Int, f_package_id: Long, f_package_name: String)


/**
  *
  * @param f_date
  * @param f_hour
  * @param f_timerange
  * @param label
  * @param f_pv
  */
case class ColumnAnaly(f_date: String, f_hour: String, f_timerange: String, f_userid: String, f_terminal: String, f_province_id: String,
                       f_city_id: String, f_region_id: String, label: String, f_pv: Int)

case class ColumnAnalyNew(f_date: String, f_hour: String, f_timerange: String, f_userid: String, f_terminal: String, f_province_id: String,
                          f_city_id: String, f_region_id: String, label: String, f_pv: Int)
/**
  *
  * @param f_column_level
  * @param f_column_id
  * @param f_column_name
  * @param f_parent_column_id
  * @param f_parent_column_name
  * @param f_parent_parent_column_id
  * @param f_parent_parent_column_name
  */
case class Column_info(f_column_level: Int, f_column_id: Long, f_column_name: String, f_parent_column_id: Long,
                       f_parent_column_name: String, f_parent_parent_column_id: Long, f_parent_parent_column_name: String)

/**
  * 业务营收，包含cp/sp、整体营收、套餐订购量、订购活跃度
  *
  * @param f_order_id          订单id
  * @param f_date              日期
  * @param f_package_id        套餐id
  * @param f_package_name      套餐名称
  * @param f_package_type      套餐类别 单片/套餐
  * @param f_province_id       省id
  * @param f_province_name     省名称
  * @param f_city_id           地市id
  * @param f_city_name         地市名称
  * @param f_region_id         区域id
  * @param f_region_name       区域名称
  * @param f_order_type        订购方式
  * @param f_order_platform    订购平台
  * @param f_order_content     订购内容
  * @param f_order_price       订单价格
  * @param f_cp_sp             cp/sp
  * @param f_payment_type      支付途径 1:全额付款 2:第三方支付
  * @param f_third_pay_detail  第三方支付明细
  * @param f_order_change_type 订购变化类型
  * @param f_pay_user_id       订购变化类型
  */
case class BusinessOperation(f_order_id: String, f_date: String, f_order_time: String, f_package_id: Long, f_package_name: String, f_package_type: Int, f_province_id: Long, f_province_name: String
                             , f_city_id: Long, f_city_name: String, f_region_id: Long, f_region_name: String, f_order_type: Int, f_order_platform: Int
                             , f_order_content: Int, f_order_price: Int, f_cp_sp: String, f_payment_type: Int, f_third_pay_detail: Int, f_order_change_type: Int
                             , f_pay_user_id: String, f_hour: Int)

/**
  * 频道在30分/60分的播放节目信息
  *
  * @param f_homed_channel_id 频道id
  * @param f_program_id       节目id
  * @param f_program_name     节目名称
  * @param f_program_hour     小时值
  * @param f_program_minute   分钟 one of 30,60
  */
case class ProgranPlayInfo(f_homed_channel_id: Long, f_program_id: Int, f_program_name: String, f_program_hour: Int, f_program_minute: Int)


/**
  *
  * @param f_date         日期
  * @param f_hour         小时
  * @param f_timerange    时刻
  * @param f_contenttype  内容类型id
  * @param f_subtype      子内容类型id
  * @param f_subtypecount 子内容类型点击次数
  * @param f_country      国家id
  * @param f_countrycount 区域点击次数
  */
case class filterInfo(f_date: String, f_hour: String, f_timerange: String, f_userid: String, f_terminal: String, f_province_id: String,
                      f_city_id: String, f_region_id: String, f_contenttype: String, f_subtype: String, f_subtypecount: Int, f_country: String, f_countrycount: Int)

/**
  *
  * @param token
  * @param keyword
  * @param date
  * @param timestamp1
  */
case class Search(token: String, keyword: String, date: String, timestamp1: Long)



case class keyWord(time: String, accesstoken: String, keyword: String)

case class WordCount(f_date: String, f_hour: String, f_timerange: String, f_userid: String, f_terminal: String, f_province_id: String,
                     f_city_id: String, f_region_id: String, f_keyword: String, f_count: Int)

