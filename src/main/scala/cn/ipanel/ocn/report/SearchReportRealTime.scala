package cn.ipanel.ocn.report

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession}
import cn.ipanel.ocn.etl.OcnConstant
import cn.ipanel.ocn.report.SearchReport.getVideoSeries
import cn.ipanel.ocn.utils.OcnLogUtils
import cn.ipanel.utils.{DBUtils, DateUtils, LogUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * SearchReportRealTime<br>
  *
  * 实时处理当天搜索数据
  *
  * @author liujjy
  * @create 2018/5/30
  * @since 1.0.0
  */
object SearchReportRealTime {
  private lazy val SPLIT = ","

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      print("参数错误,请输入topic,group_id,duration 例如:ocn_search_topic ocn_group 10")
      sys.exit(-1)
    }

    val (topic, group_id, duration) = (args(0), args(1), args(2).toInt)

    val topics = Set(topic)
    val brokers = CluserProperties.KAFKA_BROKERS
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> group_id, "auto.offset.reset" -> "largest")
    val sparkSession = SparkSession("SearchReportRealTime", "")
    val sparkContext = sparkSession.sparkContext
    //成sparkContext.setLogLevel("WARN")


    //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
    //    sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //    //开启后spark自动根据系统负载选择最优消费速率,
    //    sparkContext.getConf.set("spark.streaming.backpressure.enabled", "true")
    sparkContext.getConf.set("spark.shuffle.manager", "hash")

    //注册序列化类
    sparkContext.getConf.registerKryoClasses(Array(classOf[Log]))

    val ssc = new StreamingContext(sparkContext, if (LogUtils.isWindows()) Durations.seconds(duration) else Durations.minutes(duration))

    val logStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val data = tranform(logStream)

    foreachRdd(data, sparkContext, duration)

    ssc.start()
    //延长数据在程序中的生命周期
    //    ssc.remember(Durations.seconds(duration * 2))
    ssc.awaitTermination()

  }


  def foreachRdd(data: DStream[Log], sparkContext: SparkContext, duration: Int) = {
    data.foreachRDD(event => {
      val sqlContext = SQLContext.getOrCreate(event.sparkContext)
      import sqlContext.implicits._
      import sqlContext.sql

      val videoSeriesDF: DataFrame = getVideoSeries(sqlContext)
      videoSeriesDF.registerTempTable("t_video_series")
//      sqlContext.cacheTable("t_video_series")

      event.toDF.registerTempTable("orc_ocn_reported")

      //      videoSeriesDF.persist(StorageLevel.MEMORY_ONLY)  where  t.params['K'] is not null
      // 0122  A 表示业务搜索范围 K 表示所有关键字 R 用户通过搜索选择的媒资节目ID
      sql(
        s"""
           |select t.device_type,t.user_id,t.params['K'] key_word, t.params['R'] as result, t.params['R'] as series_id ,t.params['A'] as action,t.params['P'] as programId
           |from ( select device_type,user_id,str_to_map(params,'&',',') as params from orc_ocn_reported ) as t
       """.stripMargin).registerTempTable("t1")
      sqlContext.cacheTable("t1")

      val day = DateUtils.getNowDate("yyyy-MM-dd HH:mm")
      //      val resultDF1 = sql(
      //        s"""
      //           |select key_word as f_key_word,device_type f_device_type,series_id, count(key_word) f_pv, count(DISTINCT(key_word)) f_uv
      //           |FROM t1 GROUP by key_word,device_type,series_id
      //           """.stripMargin)
      //      resultDF1.registerTempTable("t2")
      //keyword
      val resultDF1 = sql(
        s"""
           |select key_word as f_key_word,device_type f_device_type,"" as f_program_id, count(key_word) f_pv, count(DISTINCT(user_id)) f_uv,'1' as f_word_type,
           |sum(if(action=2,1,0)) as f_click_count
           |FROM t1 where key_word is not null and programId is null GROUP by key_word,device_type
           """.stripMargin)
      //      resultDF1.registerTempTable("t2")
      //series_id
      val resultDF2 = sql(
        s"""
           |select t3.f_key_word as f_key_word,f_device_type,tt.series_id as f_program_id,f_pv,f_uv,'2' as f_word_type,f_click_count
           |from
           |(select device_type f_device_type,series_id, count(series_id) f_pv, count(DISTINCT(user_id)) f_uv,
           |sum(if(programId is not null,1,0)) as f_click_count
           |FROM t1 where series_id is not null GROUP by series_id,device_type)tt
           |join t_video_series t3
           |on tt.series_id=t3.series_id
           """.stripMargin)
      //      resultDF2.registerTempTable("t3")

      val resultDf = resultDF1.unionAll(resultDF2)
        .selectExpr(s"'$day' as f_date", s"'$duration' as duration ", "f_key_word", "f_word_type", "f_program_id", "f_pv", "f_uv",
          "f_device_type", "f_click_count")
      //      val resultDF = sql(
      //        s"""
      //           |select '$day' as f_date,'$duration' as duration, t2.f_device_type,
      //           |if(t2.series_id is null,'1','2') f_word_type,
      //           |t2.f_pv,t2.f_uv,
      //           |t2.series_id as f_program_id,
      //           |nvl(t3.f_key_word,t2.f_key_word) as f_key_word
      //           |from t2
      //           |left join t_video_series t3
      //           |on t2.series_id = t3.series_id
      //        """.stripMargin)

      DBUtils.saveToMysql(resultDf, "t_ocn_search_realtime", DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD)
      sqlContext.uncacheTable("t1")

    })
  }


  private def tranform(logSream: InputDStream[(String, String)]): DStream[Log] = {
    logSream.filter(_._2.contains(OcnConstant.SEARCH))
      .filter(line => line._2.startsWith("<?>") /* && StringUtil.isNotEmpty(line._2)*/)
      .map(x => {
        val line = x._2.substring(3)
          .replace("<", "").replace(">", "")
          .replace("[", "").replace("]", "")
          .replace("(", "").replace(")", "")
          .replace(" ", "")

        var serviceType = ""
        var deviceType = ""
        var userId = "" //用户
        var params = ""
        if (line.contains("|")) {

          val userAndDeviceStr = line.substring(0, line.indexOf("|")) //设备相关数据
          if (userAndDeviceStr.split(",").length == 5) {
            serviceType = userAndDeviceStr.split(",")(0)
            deviceType = OcnLogUtils.deviceIdMapToDeviceType(userAndDeviceStr.split(",")(4))
            userId = userAndDeviceStr.split(",")(2)
            params = line.substring(line.indexOf("|") + 1)
          }
        }

        Log(serviceType, deviceType, params, userId)
      })
      .filter(_.service_type == OcnConstant.SEARCH)
  }

  private case class Log(service_type: String = "", device_type: String = "", params: String = "", user_id: String = "")

}
