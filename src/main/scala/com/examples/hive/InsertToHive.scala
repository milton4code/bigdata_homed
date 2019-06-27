package com.examples.hive

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

case class UserStatus22(id:Int,x:Int)
object InsertToHive {
  def saveToHive(df: DataFrame,sqlContext: SQLContext): Unit ={
    df.registerTempTable("t1")
    sqlContext.sql("insert into table tt select * from t1 ")

  }

  def main(args: Array[String]): Unit = {
    val sparkSession = new SparkSession("ss")
     val sqlContext = sparkSession.sqlContext

    DBUtils.loadDataFromPhoenix(sqlContext,"youku")
      .show()


//    val querySql = s"( select * from table  where invalid = 0 and exp_date >'20190512' and operator_id =55314100 ) t"
//    val user_pay_infoDF = MultilistUtils.getMultilistData("homed_iusm", "user_pay_info", sqlContext, querySql)

//    user_pay_infoDF.show()
//    val buffer1 = new ListBuffer[UserStatus22]()
//    buffer1.append(UserStatus22(120,3))
//    buffer1.append(UserStatus22(122,4))
//    buffer1.append(UserStatus22(124,111))
//
//    val buffer2 = new ListBuffer[UserStatus22]()
//    buffer2.append(UserStatus22(125, 1))
//    buffer2.append(UserStatus22(126, 4))
//    buffer2.append(UserStatus22(127, 123))
//
//    val df1 = sparkSession.sqlContext.createDataFrame(buffer1)
//    val df2 = sparkSession.sqlContext.createDataFrame(buffer2)
//      df1.registerTempTable("t1")
////      df2.registerTempTable("t2")
//
//   val sqlContext = sparkSession.sqlContext
//    sqlContext.setConf("spark.sql.hive.metastore.version", "0.14.0")
//    sqlContext.sql("use test")
//
//    sqlContext.sql("insert overwrite table tt  select * from t1 ")
//    sqlContext.sql("insert overwrite table tt  select * from t2 ")
   /* saveToHive(df1,sparkSession.sqlContext)
    saveToHive(df2,sparkSession.sqlContext)*/


  }


}
