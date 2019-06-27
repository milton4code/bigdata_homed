package cn.ipanel.utils
import cn.ipanel.common.{CluserProperties, Constant, DBProperties}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
object RegionUtils {

  /**
    * 获取项目部署地区域
    * @return
    */
  def getRootRegion: String ={
    CluserProperties.REGION_CODE.trim
  }

  /**
    * 获取默认区域
    * @return
    */
  def getDefaultRegion():String={
    var region = ""
    val rootRegion = getRootRegion
    if(Constant.REGION_VERSION_PROVICE.equals(CluserProperties.REGION_VERSION)){
       if(rootRegion.endsWith("0000")){
         region = rootRegion.substring(0,2) +"0101"
       }else{
         region = rootRegion.substring(0,4) +"01"
       }
    }else{
      region = rootRegion
    }
    region
  }

  /**
    * 获取所有区域信息
    * @param hiveContext
    * @return  province_id, province_name,city_id,city_name,area_id as region_id,area_name region_name
    */
   def getRegions(hiveContext: HiveContext):DataFrame={
    var where_sql = ""
     if(Constant.REGION_VERSION_PROVICE.equals(CluserProperties.REGION_VERSION)){
       where_sql = s" province_id='$getRootRegion' or city_id='$getRootRegion' "
     }else{
       where_sql = s"1=1 "
     }

     hiveContext.sql(
       s"""
         | select province_id, province_name,city_id,city_name,area_id as region_id,area_name region_name
         | from bigdata.orc_area
         | where $where_sql
       """.stripMargin)

   }


}
