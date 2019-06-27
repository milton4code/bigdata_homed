package cn.ipanel.homed.general

import cn.ipanel.common.{Constant, DBProperties}
import cn.ipanel.utils.DBUtils

/**
  * 采集数据监控
  * 参数：文件路径 日期 是否清表[/xxx/xx/ 20171225 false]
  *
  * @author ZouBo
  * @date 2017/12/26 0026 
  */
object HDFSFilesSizeCollect {

  /** 1byte */
  val ONE_BYTE_SIZE: Double = 1024.0

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("参数异常,格式[date]")
      System.exit(1)
    } else {
      // 是否先清除当天数据
      val path = "/arate/"
      val date = args(0)

      // 参数[/xxxx/xxxx/date/]
      val totalSize = totalFilesSize(path + date + "/")
      println("hdfs arate当前采集到的日志数据大小：" + totalSize + " M")

      val insertSql =
        s"""insert into t_hdfs_arate_volume(f_date,f_volume)
           |values ($date,$totalSize)
        """.stripMargin
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, insertSql)
    }

  }

  /**
    * 通过shell统计目录下所有文件大小
    *
    * @param filePath 文件路径
    */
  def totalFilesSize(filePath: String) = {
    import sys.process._
    var totalSize: Double = 0.0
    val result = s"hdfs dfs -du -h -s $filePath".!!.split(Constant.FILE_PATH_SPLIT)(0).trim
    if (result.length >= 2) {
      // 大小
      val size = result.substring(0, result.length - 1).trim.toDouble
      // 单位 G/M/K
      val unit = result.substring(result.length - 1).toUpperCase()
      totalSize = unit match {
        case "G" => size * ONE_BYTE_SIZE
        case "M" => size
        case "K" => size / ONE_BYTE_SIZE
        case _ => 0.0
      }
    }
    totalSize.formatted("%.2f").toDouble
  }

}
