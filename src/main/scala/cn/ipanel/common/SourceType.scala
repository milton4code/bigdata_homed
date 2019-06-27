package cn.ipanel.common

/**
  *日志来源类型
  * create  liujjy
  * time    2019-05-06 0006 11:22
  */

//source 1 用户上报,  2 run日志 , 3 nginx日志 ,4 websocket
object SourceType {
  val USER_REPORT ="1"
  val RUN_LOG ="2"
  val NGINX ="3"
  val WEBSOCKET ="4"
}
