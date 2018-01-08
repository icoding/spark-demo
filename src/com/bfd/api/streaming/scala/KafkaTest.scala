package com.bfd.api.streaming.scala


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.slf4j.LoggerFactory

/**
  * Created by jiangnan on 18/1/7.
  */
object KafkaTest {
  val logger = LoggerFactory.getLogger("info")

  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
  val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(1))
//    ssc.checkpoint("checkpoint")
    var tt= args{0}
    tt = tt.trim()
    var table = args{1}
    val lines = KafkaUtils.createStream(ssc, "bdosn3:2181,bdosn1:2181,bdosn2:2181/kafka", "jnb2bc1", Map[String, Int]("apache-logs" -> 1)).map(_._2)
    lines.foreachRDD((x: RDD[String],time: Time) =>{
      x.foreachPartition{res =>
      {
        if(!res.isEmpty){
          val connection = ConnectionPool.getConnection.getOrElse(null)

          res.foreach {
            r: String =>
              val bbd = JSON.parseObject(r)
              if(tt.equals(bbd.get("STATUS"))){
                val userstatqusql = "insert into "+table+" (kafka_value) values (?)"
                val userqustmt = connection.prepareStatement(userstatqusql);
                userqustmt.setString(1,r );
                val userquCount = userqustmt.executeUpdate();
              }

          }
          ConnectionPool.closeConnection(connection)
        }
      }
      }
    })
//      lines.foreachRDD(rdd=>{
//      write(rdd.name)
//
//    })
//    val words = lines.flatMap(x=> {
//      write(x)
//       x.split(" ");
//    }
//    )
//    val wordCounts = words.map(x =>(x, 1L))
////      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(2), 2)
//    wordCounts.print()

//    words.print()
//    val wordCounts = words.map(x => (x, 1L))
//    wordCounts.print()
//    wordCounts.map(x => )
//    bbq.foreachRDD((rdd:RDD[String], time: Time)=> {
////      System.out.print(rdd.isEmpty())
//      rdd.foreach(pair=>{
//        write(pair)
//
//      })
//      rdd.foreachPartition(partitionOfRecords => {
//        if (partitionOfRecords.isEmpty) {
//        } else {
//          partitionOfRecords.foreach(pair => {
//            write(pair)
//
//          })
//        }
//      })
//    })
//    val words = lines.flatMap(_.split(" "))
////    words.map(x=>{
////      write(x)
////    })
//    val wordCounts = words.map(x =>  write(x))
//    wordCounts.print()

    //    lines.foreachRDD((rdd: RDD[String],time:Time)=>{
//      val r1= rdd.collect()
//      if(r1.length>0){
//        val w = r1{0};
//        val connection = ConnectionPool.getConnection.getOrElse(null)
//        val userstatqusql = "insert into kafka_to_spark (kafka_value) values (?)"
//        val userqustmt = connection.prepareStatement(userstatqusql);
//        userqustmt.setString(1,w+"" );
//        val userquCount = userqustmt.executeUpdate();
//      }

//    })

//    lines.foreachRDD((rdd: RDD[String])=>{
//      logger.info("tdsdsdssd")
//      rdd.foreach(xxx=>{
//        val connection = ConnectionPool.getConnection.getOrElse(null)
//        val userstatqusql = "insert into kafka_to_spark (kafka_value) values (?)"
//        val userqustmt = connection.prepareStatement(userstatqusql);
//        userqustmt.setString(1, xxx);
//        val userquCount = userqustmt.executeUpdate();
//      })
//      rdd.foreachPartition(ppp=>{
//        if (ppp.isEmpty) {
//          logger.info("this is RDD(userCountsRowRDD)  is not null but partition is null")
//                        println("this is RDD(userCountsRowRDD)  is not null but partition is null")
//                      } else {
//          val connection = ConnectionPool.getConnection.getOrElse(null)
//          ppp.foreach(x => {
//            val userstatqusql = "insert into kafka_to_spark (kafka_value) values (?)"
//            val userqustmt = connection.prepareStatement(userstatqusql);
//            userqustmt.setString(1, x);
//            val userquCount = userqustmt.executeUpdate();
//            System.out.println(x)
//
//
//          })
//          ConnectionPool.closeConnection(connection)
//        }
//      })

//    })
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(2), 2)
//    wordCounts.foreachRDD()
    ssc.start()
    ssc.awaitTermination()
  }
  def write(w:String): Unit ={
    val connection = ConnectionPool.getConnection.getOrElse(null)
            val userstatqusql = "insert into kafka_to_spark (kafka_value) values (?)"
            val userqustmt = connection.prepareStatement(userstatqusql);
            userqustmt.setString(1,w );
            val userquCount = userqustmt.executeUpdate();
//    return 1;
  }

}
