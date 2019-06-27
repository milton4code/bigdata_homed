package cn.ipanel.etl

import java.util

import org.apache.spark.sql.hive.HiveContext

/**
  *app版本初始化工具
  * create  liujjy
  * time    2019-04-18 0018 13:44
  */
object AppVersionTools {

  /**
    * 获取默认版本信息
    * @param hiveContext
    * @return  两种形式的kv组合 1.硬件版本-> 机型, 2 机型->硬件版本, 3 机型_V -> 厂商 4 硬件版本_V -> 厂商
    */
   def getDefaultVersion(hiveContext: HiveContext):java.util.Map[String,String]={
     var map = new util.HashMap[String,String]()
      hiveContext.sql(" select  model,hard_version,vender  from bigdata.app_version")
       .collect().foreach(r=>{
        val m = r.getString(0).trim //机型
        val h = r.getString(1).trim //硬件版本
        val vender = r.getString(2).trim  //厂商

        map.put(m,h)
        map.put(h,m)
        map.put(m+"_V",vender)
        map.put(h+"_V",vender)
      })

     map
   }

}
