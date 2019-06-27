package cn.ipanel.rank.common

/**
  *
  *
  * @author ZouBo
  * @date 2018/6/6 0006 
  */
private[rank] object Constant {
  //排行榜统计周期
  val RANK_PERIOD = 7.00

  //频道排行前10
  val CHANNEL_RANK_TOP_TEN = 10
  /** 频道排行STB端 */
  val CHANNEL_RANK_STB = "1"
  /** 频道排行MOB端 */
  val CHANNEL_RANK_MOB = "3"
  /** 频道排行PC端 */
  val CHANNEL_RANK_PC = "5"
  //排行榜标识
  val CHANNEL_RANK_SYMBOL = "rank"
  //redis数据过期时间
  val CHANNEL_RANK_EXPIRE_TIME = "691200"
  //redis库
  val REDIS_DB_NUM = 5

  val REDIS_DB_NAME = "dtvs_user_often_watch_channel"

  //导演
  val ACTOR_RULE_DIRECTOR = "director"
  //领衔主演
  val ACTOR_RULE_FIRST = "first"
  //主演
  val ACTOR_RULE_SECOND = "second"
  //配演
  val ACTOR_RULE_OTHER = "other"
  //演员角色占统计量占比--导演
  val ACTOR_PROPORTION_DIRECTOR: Double = 0.20
  //演员角色占统计量占比--领衔主演
  val ACTOR_PROPORTION_FIRST: Double = 0.40
  //演员角色占统计量占比--主演
  val ACTOR_PROPORTION_SECOND: Double = 0.30
  //演员角色占统计量占比--主演
  val ACTOR_PROPORTION_OTHER: Double = 0.10
  //明星相关媒资热度网络播放量占比
  val ACTOR_RANK_NET_PLAY_PROPORTION: Double = 0.5
  //明星相关媒资热度本地播放量占比
  val ACTOR_RANK_LOCAL_PLAY_PROPORTION: Double = 0.5
  //明星相关媒资热度网络搜索量占比
  val ACTOR_RANK_NET_SEARCH_PROPORTION: Double = 0.5
  //明星相关媒资热度本地搜索量占比
  val ACTOR_RANK_LOCAL_SEARCH_PROPORTION: Double = 0.5
  //剧集新度占比
  val ACTOR_RANK_SERIES_NEW_HEAT: Double = 0.2
  //剧集新度值-最近7天
  val ACTOR_SERIES_NEW_HEAT_7: Double = 1.0
  //剧集新度值-最近30天
  val ACTOR_SERIES_NEW_HEAT_30: Double = 0.5
  //剧集新度值-超过30天
  val ACTOR_SERIES_NEW_HEAT_BEYOND_30: Double = 0
  //喜爱搜藏占比
  val ACTOR_RANK_LIKE_ENSHRINE_PROPORTITION: Double = 0.2
  //追剧占比
  val ACTOR_RANK_FAVORITE_PROPORTITION: Double = 0.2
  //搜索量占比
  val ACTOR_RANK_SEARCH_PROPORTITION: Double = 0.2
  //播放量占比
  val ACTOR_RANK_PLAY_PROPORTION: Double = 0.2
  //明星热度-关注量占比
  val ACTOR_RANK_ATTENTION_PROPORTION: Double = 0.35
  //明星热度-相关媒资热度占比
  val ACTOR_RANK_MEDIA_HEAT_PROPORTION: Double = 0.25
  //明星热度-搜索量占比
  val ACTOR_RANK_SEARCH_PROPORTION: Double = 0.20
  //明星热度-点击量占比
  val ACTOR_RANK_CLICK_PROPORTION: Double = 0.15
  //明星热度-网络关注量占比
  val ACTOR_RANK_NET_ATTENTION_PROPORTION: Double = 0.05
  //明星热度统计周期-1日榜
  val ACTOR_RANK_PERIOD_TYPE_DAY: Int = 1
  //明星热度统计周期-2周榜
  val ACTOR_RANK_PERIOD_TYPE_WEEK: Int = 2
  //明星热度统计周期-3月榜
  val ACTOR_RANK_PERIOD_TYPE_MONTH: Int = 3

  //==================================
  //redis 分隔符
  val DECOLLATOR = "_"

  val CHANNEL_RANK_PUT_URL=""


}
