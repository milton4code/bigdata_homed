package cn.ipanel.homed.realtime

/**
  * 场景切换测试
  */
object OnlineTest extends App {

  val cache = Set("1-2-true","2-1-true","5-3-true","5-2-true")
  val log   =Set("1-2-false","2-1-true","2-2-true","4-3-false","4-2-true")
  //第一步 取日志缓存中都有
  val bothIn= cache & log

  //第二步 取日志中有,缓存中没有
  val inLogNotCache = log &~ cache
  //得到在日志中状态为true的数据,表示新的用户加入 2-2-1 ,4-2-1 ok
  val onlySuccess =inLogNotCache.filter(_.contains("true")) // 0k
 // 得到只包含false的,(用来判断缓存中可能切换场景的用户)
  val onlyFinish = inLogNotCache.filter(_.contains("false"))

  //第三步 取日志中没有,缓存中有
  // 这一步需要去掉场景切换的用户,需要结合onlyFinish数据
  val inCacheNotLog =  cache &~ log

  val _onlyU = inCacheNotLog.map(k => k.substring(0,k.lastIndexOf("-"))) &~
    onlyFinish.map(k => k.substring(0,k.lastIndexOf("-")))

  val onlyU =_onlyU.map(_.concat("-true"))

  println(onlyU)

  val result = bothIn.union(onlySuccess).union(onlyU)
  println(result)
}
