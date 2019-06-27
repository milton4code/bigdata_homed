package com.examples

import cn.ipanel.common.SparkSession
import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}


object CustomDirectKafkaExample {

  val sparkSession = new SparkSession("abc","")
  val  msc = sparkSession.sparkContext
  val logger = Logger.getLogger(CustomDirectKafkaExample.getClass)

  def main(args: Array[String]) {
    val checkpointDir = "/spark/ck"
    val topic = "run_log_topic"
    val topicsSet = Set(topic)
    val brokers = "192.168.18.60:9092,192.168.18.61:9092,192.168.18.63:9092"
    val group_id = "channel_live_group"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder","group.id"->group_id, "auto.offset.reset"->"largest")

    val zkHosts= "192.168.18.60:2181,192.168.18.61:2181,192.168.18.63:2181/kafka"
    val zkPath = "/bigdata/channel_live/offset"

    val ssc = new StreamingContext(msc, Durations.seconds(5))
    val eventStream = createCustomDirectKafkaStream(ssc,kafkaParams,zkHosts,zkPath,topicsSet)

    eventStream.foreachRDD(rdd=>{

      rdd.flatMap(line=>{
        val data = JSON.parseObject(line._2)
        println("data=====" + data)
        Some(data)
      })

    })
    ssc.start()
    ssc.awaitTermination();
  }

    /* createDirectStream() method overloaded */
    def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String
                                      , zkPath: String, topics: Set[String]): InputDStream[(String, String)] = {
      val topic = topics.last //TODO only for single kafka topic right now
      val zkClient = new ZkClient(zkHosts, 30000, 30000)
      val storedOffsets = readOffsets(zkClient,zkHosts, zkPath, topic)
      val kafkaStream = storedOffsets match {
        case None => // start from the latest offsets
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        case Some(fromOffsets) => // start from previously saved offsets
          val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
      }
      // save the offsets
      kafkaStream.foreachRDD(rdd => { saveOffsets(zkClient,zkHosts, zkPath, rdd) })
      kafkaStream
    }

    /*
     Read the previously saved offsets from Zookeeper
      */
    private def readOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, topic: String): Option[Map[TopicAndPartition, Long]] = {
      logger.info("Reading offsets from Zookeeper")
      val stopwatch = new Stopwatch()
      val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
      println("========readOffsets==============" + offsetsRangesStrOpt)

      offsetsRangesStrOpt match {
        case Some(offsetsRangesStr) =>
          logger.info(s"Read offset ranges: ${offsetsRangesStr}")
          val offsets = offsetsRangesStr.split(",")
            .map(s => s.split(":"))
            .map { case Array(partitionStr, offsetStr) => TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
            .toMap
          logger.info("Done reading offsets from Zookeeper. Took " + stopwatch)
          Some(offsets)
        case None =>
          logger.info("No offsets found in Zookeeper. Took " + stopwatch)
          None
      }
    }

    private def saveOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, rdd: RDD[_]): Unit = {
      println("========saveOffsets==================" + zkClient)

      logger.info("Saving offsets to Zookeeper")
      val stopwatch = new Stopwatch()
      val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsRanges.foreach(offsetRange => logger.warn(s"Using======================== ${offsetRange}"))
      val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}") .mkString(",")
      logger.info("chandan Writing offsets to Zookeeper zkClient="+zkClient+"  zkHosts="+zkHosts+"  zkPath="+zkPath+"  offsetsRangesStr:"+ offsetsRangesStr)
      ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
//      ZkUtils.updatePersistentPath()
      logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
    }

    class Stopwatch {
      private val start = System.currentTimeMillis()
      override def toString() = (System.currentTimeMillis() - start) + " ms"
    }
  }

