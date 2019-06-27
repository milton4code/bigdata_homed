package cn.ipanel.rank.reports

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.rank.common.Constant
import cn.ipanel.rank.reports.StarRankWeek._
import cn.ipanel.utils.{DBUtils, DateUtils}

/**
  *
  * 明星热度月榜
  *
  * @author ZouBo
  * @date 2018/6/25 0025 
  */
object StarRankMonth {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("请输入正确参数[yyyyMMdd]")
      System.exit(1)
    }
    //月榜最后一日
    val lastDay = args(0).trim
    //月榜第一日
    val firstDay = DateUtils.getNDaysAfter(-30, lastDay)
    val session = SparkSession("StarRankMonth", "")
    val result = getActorRankData(session, firstDay, lastDay)
    DBUtils.saveToMysql(result, "t_start_heat_rank", DBProperties.JDBC_URL_RANK, DBProperties.USER_RANK, DBProperties.PASSWORD_RANK)
  }

  /**
    * 获取明星热度数据
    *
    * @param session
    * @param firstDay
    * @param lastDay
    */
  def getActorRankData(session: SparkSession, firstDay: String, lastDay: String) = {
    //1.搜索量、关注量、点击量
    val sac = loadSearchAttentionClick(firstDay, lastDay, session)
    //2.网络关注量
    val netAttention = loadNetAttention(firstDay, lastDay, session)
    //3.相关媒资热度
    val starHeat = computeMediaHeat(session, loadMediaHeat(firstDay, lastDay, session,3), lastDay)
    processDF(sac, netAttention, starHeat, session, Constant.ACTOR_RANK_PERIOD_TYPE_MONTH)

  }

}
