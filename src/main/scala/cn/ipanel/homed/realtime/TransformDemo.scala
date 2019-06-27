/*
package cn.ipanel.homed.realtime

import cn.ipanel.common.SparkSession

/**
  *
  *
  * @author liujjy  
  * @date 2018/02/07 16:32
  */

object TransformDemo extends App {
  val sparkSes = SparkSession("", "")
  val sc = sparkSes.sparkContext

  import org.apache.spark.streaming.{Seconds, StreamingContext}

  val ssc = new StreamingContext(sc, batchDuration = Seconds(5))

  //  val ns = sc.parallelize(0 to 2)
  //  import org.apache.spark.streaming.dstream.ConstantInputDStream
  //  val nums = new ConstantInputDStream(ssc, ns)

  val _users = sc.parallelize(Seq(User2("zero", 1), User2("one", 2), User2("two", 3)))

  import org.apache.spark.streaming.dstream.ConstantInputDStream

  val users = new ConstantInputDStream(ssc, _users)

  val _logs = sc.parallelize(Seq(Log2("zer2", 1), Log2("one2", 2), Log2("two2", 3)))

  import org.apache.spark.streaming.dstream.ConstantInputDStream

  val lgos = new ConstantInputDStream(ssc, _logs)

  import org.apache.spark.rdd.RDD
  import org.apache.spark.streaming.Time



 /* val transformedFileAndTime = fileAndTime.transformWith(anomaly, (rdd1: RDD[(String,String)], rdd2 : RDD[Int]) => {
    var first = " ";
    var second = " ";
    var third = 0
    if (rdd2.first<=3)  {
      first = rdd1.map(_._1).first
      second = rdd1.map(_._2).first
      third = rdd2.first
    }
    Rdd[(1,2,3)]
  })
  */

  //  val transformFunc: (RDD[Int], RDD[String], Time) => RDD[(Int,String)] = { case (ns, ws, time) =>
  //    println(s">>> ns: $ns")
  //    println(s">>> ws: $ws")
  //    println(s">>> batch: $time")
  //    ns.zip(ws)
  //  }
  users.transformWith(lgos, (rdd1:RDD[User2],rdd2:RDD[Log2],time:Time)=>{

  ssc.start()
  ssc.awaitTermination()

}


case class User2(name: String, age: Int)

case class Log2(device: String, userId: Int)
*/
