package cn.ipanel.utils

/**
  * 日志处理工具
  *
  * @author liujjy  
  * @date 2017/12/26 15:07
  */

object LogTools {

  /**
    * 解析日志中 以&分隔的key-value
    * @param paras  &分隔的key-value 字符串
    * @return  map结构数据
    */
  def parseMaps(paras: String,separator1:String=",",separator2:String=" "): scala.collection.mutable.Map[String, String] = {
    import scala.collection.mutable.Map
    val map = Map[String, String]()
    val arr = paras.split(",")
    for (i <- arr) {
      val kv = i.split(" ")
      if (kv.length == 2) {
        map += (kv(0) -> kv(1))
      } else if (kv.length == 1){
        map += (kv(0)->"")
      } else{
      }
    }
    map
  }

}
