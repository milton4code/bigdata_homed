package cn.ipanel.ocn.task

import cn.ipanel.utils.PushTools

object PustTest {

  def main(args: Array[String]): Unit = {

    val data = "aaa|bb"
    PushTools.sendPostRequest("http://118.24.221.30:10086",data)
  }
}
