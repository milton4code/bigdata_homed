package cn.ipanel.etl

import java.sql.DriverManager
import java.util

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.homed.repots.SearchDetailNew.str_to_map
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** *
  * 湖南项目对上报日志进行过滤 仅保留 凤凰专区的 0104、0131、0701 数据
  */
object ArateFilter {
  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      println("参数错误,请输入日期 ，统计开始时间，统计结束时间 ,栏目id（若传多个栏目id则中间用逗号分割 48118,50508）,文件保存路径 用户订购行为保存路径 ,输出文件个数   " +
        "如 20180528 48118 huan/ArateFilter/comment huan/ArateFilter/order 1")
      sys.exit(-1)
    }

    val day = args(0)
    val startTime = args(1).toLong //统计开始时间
    val endTime = args(2).toLong // 统计结束时间
    val specialColum = args(3) // 要统计的指定栏目
    val filePath = args(4) // 文件保存路径
    val filePathOrder = args(5) //带订购信息的文件保存路径
    val fileNum = args(6).toInt //输出文件的个数
    val columnArr = specialColum.split(",")
    val list = getColumnInfo(columnArr).sorted // 拿到指定栏目的下所有子栏目
    val session = new SparkSession("ArateFilter", "")
    val sc = session.sparkContext
    lazy val sqlContext = session.sqlContext
    import sqlContext.implicits._
    val columnMap = new mutable.HashMap[String, String]()
    val path = LogConstant.HDFS_ARATE_LOG + day + "/*"
    val textFileRdd = sc.textFile(path).filter(x => (x.startsWith("<?><[0104") && x.contains("<(CL,") && x.contains("<(ID,")) ||
      (x.startsWith("<?><[0131") && x.contains("<(ID,")) ||
      (x.startsWith("<?><[0701") && x.contains("<(I,") && (x.contains("<(S,4)>") || x.contains("<(S,6)>"))))
      .map(x => {
        var columnId = "0" //栏目id
        var action = "0" //action类型
        var reportTime = 0L //上报时间
        val log = x.substring(3).replace("<", "")
          .replace(">", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")
        if (log.contains("|")) {
          val arr = log.split("\\|")
          if (arr.length == 2) {
            reportTime = arr(0).split(",")(1).replaceAll("\\D", "").toLong
            val expandDate = arr(1)
            val maps = str_to_map(expandDate)
            if (x.startsWith("<?><[0104") && maps.contains("ID")) {
              action = maps.getOrElse("A", "0")
              val programID = maps("ID")
              val tmp_column = maps.getOrElse("CL", "0")
              columnId = if (tmp_column.equals("null") || tmp_column.equals("undefined")) "0" else tmp_column
              columnMap.put(programID, columnId)
            } else if (x.startsWith("<?><[0131")) {
              action = maps.getOrElse("A", "0")
              val tmp_column = maps.getOrElse("ID", "0")
              columnId = if (tmp_column.equals("null") || tmp_column.equals("undefined")) "0" else tmp_column
            } else if (x.startsWith("<?><[0701") && x.contains("<(S,4)>") && maps.contains("I")) {
              val programID = maps("I")
              columnId = columnMap.getOrElse(programID, "0")
            } else if (x.startsWith("<?><[0701") && x.contains("<(S,6)>")) {
              val tmp_column = maps.getOrElse("I", "0")
              columnId = if (tmp_column.equals("null") || tmp_column.equals("undefined")) "0" else tmp_column
            }
          }
        }
        (reportTime, columnId, action, x)
      }).filter(x => list.contains(x._2) && x._1 >= startTime && x._1 <= endTime)

    textFileRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val behaviorData = textFileRdd.filter(x => x._3 != "8").toDF("reportTime", "columnid", "action", "content").select("content")
    val orderData = textFileRdd.filter(x => x._3 == "8").toDF("reportTime", "columnid", "action", "content").select("content")
    behaviorData.repartition(fileNum).write.format("text").text(s"$filePath") // 用户普通行为的数据保存到hdfs
    orderData.repartition(fileNum).write.format("text").text(s"$filePathOrder") // 用户订购行为的数据保存到hdfs
    textFileRdd.unpersist()
  }

  /** *
    * 获取凤凰专区栏目(48118 50508)下所有子栏目信息
    *
    */
  def getColumnInfo(columnArr: Array[String]): ListBuffer[String] = {
    val map = new java.util.HashMap[Long, Long]()
    val list = new ListBuffer[String]
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(
      "SELECT f_column_id,f_parent_id FROM	`t_column_info` WHERE f_is_hide = 0")
    while (queryRet.next) {
      val f_column_id = queryRet.getLong("f_column_id")
      val f_parent_id = queryRet.getLong("f_parent_id")
      map.put(f_column_id, f_parent_id)
    }
    connection.close
    for (e <- columnArr) {
      lists.clear()
      val key = e.trim.toLong
      list += key.toString
      val temList = getSpecialColumn(key, map)
      list ++= temList
    }
    list
  }

  val lists = new ListBuffer[String]

  def getSpecialColumn(columId: Long, map: util.HashMap[Long, Long]): ListBuffer[String] = {
    val list = new ListBuffer[Long]
    var tmp_columnId = columId
    val sets = map.entrySet().iterator()
    while (sets.hasNext) {
      val date = sets.next()
      val key = date.getKey
      val value = date.getValue
      if (value == tmp_columnId) {
        lists += key.toString
        list += key
      }
    }
    for (e <- list) {
      getSpecialColumn(e, map)
    }
    lists
  }
}
