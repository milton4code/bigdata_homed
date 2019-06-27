package cn.ipanel.utils

import java.sql.{DriverManager, PreparedStatement, SQLException}
import java.util.Properties

import cn.ipanel.common.{CluserProperties, DBProperties}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.phoenix.jdbc.PhoenixConnection
import org.apache.phoenix.query.QueryServices
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.tephra.TxConstants.HBase

/**
  * 数据库工具
  *
  * @author liujjy
  * @date 2017/12/25 14:22
  */

object DBUtils {


  /**
    * 以原生jdbc方式执行insert/delete
    *
    * @param url      url
    * @param user     userName
    * @param password 密码
    * @param sql      sql语句
    */
  def executeSql(url: String, user: String, password: String, sql: String) = {

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection(url, user, password)
    val ps = connection.prepareStatement(sql)
    ps.execute()
    ps.close()
    connection.close()
  }

  /**
    * 从数据库中加载数据
    *
    * @param sqlContext sqlContext
    * @param tableName  表名(sql语句的话一定要写封装成临时表)
    * @param url        url
    * @param user       user
    * @param password   password
    * @return
    */
  def loadMysql(sqlContext: SQLContext, tableName: String, url: String, user: String, password: String) = {
    sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> user,
        "password" -> password,
        "dbtable" -> tableName)).load()
  }

  /**
    * 数据保存到homed_data2数据库中
    *
    * @param dataFrame dataFrame
    * @param table     tableName  tableName
    */
  def saveToHomedData_2(dataFrame: DataFrame, table: String) = {
    val prop = new java.util.Properties
    prop.setProperty("user", DBProperties.USER)
    prop.setProperty("password", DBProperties.PASSWORD)
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    dataFrame.write.mode(SaveMode.Append).jdbc(DBProperties.JDBC_URL, table, prop)
  }

  /**
    * 数据保存到homed_data2数据库中
    * 覆盖原有数据
    *
    * @param dataFrame dataFrame
    * @param table     tableName  tableName
    */
  def saveToHomedData_Overwrite(dataFrame: DataFrame, table: String) = {
    val prop = new java.util.Properties
    prop.setProperty("user", DBProperties.USER)
    prop.setProperty("password", DBProperties.PASSWORD)
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    dataFrame.write.mode(SaveMode.Overwrite).jdbc(DBProperties.JDBC_URL, table, prop)
  }

  /**
    * 保存数据到mysql
    *
    * @param dataFrame dataFrame
    * @param table     表
    * @param url       url
    * @param user      user
    * @param password  password
    */
  def saveToMysql(dataFrame: DataFrame, table: String, url: String = DBProperties.JDBC_URL, user: String = DBProperties.USER, password: String = DBProperties.PASSWORD) {
    val prop = new java.util.Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    dataFrame.coalesce(10).write.mode(SaveMode.Append).jdbc(url, table, prop)
  }


  /**
    * 保存数据到hbase
    *
    * @param sc        SparkContext
    * @param tableName 表名
    * @param rdd       rdd
    */
  def saveToHbase(sc: SparkContext, tableName: String, rdd: RDD[(ImmutableBytesWritable, Put)]) {
    initHbaseConfig(sc)
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(sc.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }

  private[this] def initHbaseConfig(sc: SparkContext) = {
    sc.hadoopConfiguration.set(HBase.ZOOKEEPER_QUORUM, CluserProperties.ZOOKEEPER_URL)
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", CluserProperties.HBASE_CLIENT_PORT)
  }


  /**
    * 保存数据到Phoenix
    *
    * @param dataFrame dataFrame
    * @param tableName 表名
    */
  def saveDataFrameToPhoenixNew(dataFrame: DataFrame, tableName: String) = {
    dataFrame.write.format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      //      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("zkUrl", CluserProperties.ZOOKEEPER_URL)
      .option("table", tableName)
      .save()
  }

  /**
    * 保存数据到Phoenix
    *
    * @param dataFrame dataFrame
    * @param tableName 表名
    */
  //@deprecated("过时的方法，不推荐使用")
  def saveDataFrameToPhoenixOld(dataFrame: DataFrame, tableName: String) = {
    dataFrame.save("org.apache.phoenix.spark",
      SaveMode.Overwrite,
      Map("table" -> tableName, "zkUrl" -> CluserProperties.ZOOKEEPER_URL))
  }

  /**
    * 从phoenix加载数据
    * 方式一、传表名
    *
    * @param sqlContext
    * @param table
    * @return
    */
  def loadDataFromPhoenix(sqlContext: SQLContext, table: String) = {
    sqlContext.read.format("org.apache.phoenix.spark")
      .option("table", table)
      .option("zkUrl", CluserProperties.PHOENIX_ZKURL)
      .load
  }

  /**
    * 方式二：传sql查询语句
    *
    * @param sqlContext
    * @param table
    * @return
    */
  def loadDataFromPhoenix2(sqlContext: SQLContext, table: String) = {
    sqlContext.read.format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("dbtable", table)
      .option("url", CluserProperties.PHOENIX_ZKURL)
      .load
  }

  /**
    * JDBC方式执行SQL操作phoenix
    * @author lizhy@20181211
    * @param sql SQL语句
    */
  def excuteSqlPhoenix(sql:String)= {
    var conn: PhoenixConnection = null
    var stmt: PreparedStatement = null
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(CluserProperties.PHOENIX_ZKURL).asInstanceOf[PhoenixConnection]
      stmt = conn.prepareStatement(sql)
      stmt.execute()
      conn.commit()
      println("sql执行成功!")
    } catch {
      case ex: Exception => {
        println("sql执行失败!")
        ex.printStackTrace()
      }
    } finally try {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }

  /**
    * JDBC方式执行SQL操作phoenix
    * @author lizhy@20181211
    * @param sql SQL语句
    */
  def excuteSqlPhoenixNew(sql:String)= {
    var conn: PhoenixConnection = null
    var stmt: PreparedStatement = null
    try {
      val connectionProperties = new Properties();
      connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"10000000");
      connectionProperties.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"10000000");
      conn = DriverManager.getConnection(CluserProperties.PHOENIX_ZKURL,connectionProperties).asInstanceOf[PhoenixConnection]
      stmt = conn.prepareStatement(sql)
      stmt.execute()
      conn.commit()
      println("sql执行成功!")
    } catch {
      case ex: Exception => {
        println("sql执行失败!")
        ex.printStackTrace()
      }
    } finally try {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }
}
