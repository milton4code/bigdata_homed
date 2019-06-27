package cn.ipanel.rank.reports

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils, PropertiUtils, RedisClient}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object LabelFromMysql {
  private val pro = PropertiUtils.init("redis.properties")

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("LabelFromMysql")
    // val sc = getSparkContext("LabelFromMysql")
    val sc = sparkSession.sparkContext
    //val sqlContext = new SQLContext(sc)
    val sqlContext = sparkSession.sqlContext
    val date = args(0)
    val startDate = DateUtils.getDateByDays(date, 29)
    val endMonth = DateUtils.getMonthEnd(date: String)
    getLabelFromMysql(sqlContext)
    // getLabelFromMysqlByMonth(sqlContext)
    sendSuccessMessage()
  }


  def sendSuccessMessage() = {
    val url = CluserProperties.USER_PUT_URL
    try {
      val httpClient = HttpClients.createDefault()
      val httpGet = new HttpGet(url)
      val response = httpClient.execute(httpGet)
      val httpEntity = response.getEntity
      if (null != httpEntity) {
        val result = EntityUtils.toString(httpEntity, "UTF-8")
        EntityUtils.consume(httpEntity)
        println("请求通讯[" + url + "]成功:" + result)
      }
    } catch {
      case e: Exception => println("请求通讯[" + url + "]异常,异常信息:" + e.printStackTrace())
    }
  }


  def getSparkContext(name: String) = {
    val sparkConf = new SparkConf().setAppName(name).setMaster("local[2]")
    System.setProperty("hadoop.home.dir", "G:\\hadoop\\hadoop-2.6.4")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.akka.frameSize", "500")
    sparkConf.set("spark.rpc.askTimeout", "30")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    sc
  }


  def getLabelFromMysqlByMonth(sqlContext: SQLContext) = {
    val sql =
      """
        |(select *
        | from t_user_profile_label_by_month t) as aa
      """.stripMargin
    val LabelMonth = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
    LabelMonth.na.fill("").registerTempTable("labelbymonth")
    val label = sqlContext.sql(
      """
select t.f_user_id,
        |concat('{',
        |'"statistics.duration":"',t.f_date,
        |'","statistics.active_days":"',t.active_days,
        |'","statistics.online_duration":"',t.online_duration,
        |'","statistics.average_duration":"',t.average_duration,
        |'","statistics.contenttype_ratio":[',t.contenttype_ratio,
        |'],"statistics.monetary":"',t.monetary,
        |'","statistics.frenquency":"',t.frenquency,
        |'","statistics.often_channel":"',t.often_channel,
        |'","statistics.contentview":"',t.contentview,
        |'"}') as json
        |from labelbymonth t
      """.stripMargin)
    val redis = new Jedis(pro.getProperty("redis_host"), pro.getProperty("redis_port").toInt)
    //     val redis = RedisClient.getRedis()
    label.collect().map(r => {
      redis.hset("ius_user_profile_2".getBytes, r.getAs[String]("f_user_id").getBytes(), r.getAs[String]("json").getBytes())
    })
    redis.close()
  }


  def getLabelFromMysql(hiveContext: SQLContext) = {
    val sql =
      """
        |(select *
        | from t_user_profile_label t) as aa
      """.stripMargin
    val Label = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
    Label.na.fill("").registerTempTable("label")
    val label = hiveContext.sql(
      """
        |select t.f_user_id,
        |concat('{"star":"',t.star,
        |'","country":"',t.country,
        |'","channel":"',t.channel,
        |'","mediatag":[',t.media,
        |'],"freshness ":"',t.freshness,
        |'","mediainterestseries":"',t.mediainteresseries,
        |'","mediainterestprogramtype":"',t.mediainteresprogramtype,
        |'","group":"',t.group,
        |'","ctivity":"',t.activity,
        |'","bterminal":"',t.terminal,
        |'","section":"',t.section,
        |'","duration":"',t.duration,
        |'","times":"',t.times,
        |'","porfile_extern":{',t.porfile_extern,'}}') as json
        |from  label t
      """.stripMargin)
    val redis = new Jedis(pro.getProperty("redis_host"), pro.getProperty("redis_port").toInt)
    // val redis = RedisClient.getRedis()
    label.collect().map(x => {
      redis.hset("ius_user_profile_1".getBytes, x.getAs[String]("f_user_id").getBytes(), x.getAs[String]("json").replace("\\", "") getBytes())
    })
    redis.close()
  }


}
