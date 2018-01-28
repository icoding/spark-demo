package com.bfd.api.streaming.scala

import java.io._
import java.util.zip.ZipInputStream
import java.util.{Properties, Scanner}

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source
/**
  * Created by jiangnan on 18/1/7.
  */
object KafkaFileArchivesTest {
  val logger = LoggerFactory.getLogger("info")

  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
  val sparkConf = new SparkConf()


    val ssc = new StreamingContext(sparkConf, Seconds(50))
//    ssc.checkpoint("checkpoint")
    var tt= args{0}
    tt = tt.trim()
    var table = args{1}
    var group = args{2}
    group = group.trim()
    var topic = args{3}
    topic = topic.trim()

    val filePath ="ip.txt"
    val agezip = "age.zip"

    val props = new Properties()
    props.load(new FileInputStream(filePath))
    val agemap = readDir(agezip)

    val lines = KafkaUtils.createStream(ssc, "bdosn3:2181,bdosn1:2181,bdosn2:2181/kafka", group, Map[String, Int](topic -> 1)).map(_._2)
    lines.foreachRDD((x: RDD[String],time: Time) =>{
      x.foreachPartition{res =>
      {
        if(!res.isEmpty){
          val connection = ConnectionPool.getConnection.getOrElse(null)

          res.foreach {
            r: String =>
              val json = JSON.parseObject(r)
              if(tt.equals(json.get("STATUS"))){
                val userstatqusql = "insert into "+table+" (kafka_value) values (?)"
                val userqustmt = connection.prepareStatement(userstatqusql);
                userqustmt.setString(1,r );
                val userquCount = userqustmt.executeUpdate();
              }
              var ip = props.getProperty("ip")
              if(ip!=null){
                if(ip.equals(json.get("ip"))){
                  val userstatqusql = "insert into "+table+" (kafka_value) values (?)"
                  val userqustmt = connection.prepareStatement(userstatqusql);
                  userqustmt.setString(1,r );
                  val userquCount = userqustmt.executeUpdate();
                }
              }
              var age = agemap.get("age")
              println("age:"+age)

              if(age!=null){
                if(age.equals(json.get("age"))){
                  val userstatqusql = "insert into "+table+" (kafka_value) values (?)"
                  val userqustmt = connection.prepareStatement(userstatqusql);
                  userqustmt.setString(1,r );
                  val userquCount = userqustmt.executeUpdate();
                }
              }
          }
          ConnectionPool.closeConnection(connection)
        }
      }
      }
    })
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
  def readDir(archives:String): mutable.HashMap[String, String] ={
    val dir = new File(archives  )
    val docs = scala.collection.mutable.HashMap[String, String]()

    for (f <- dir.listFiles()) yield {
      val lines = Source.fromFile(f.getCanonicalPath).getLines()
      for(line <- lines)
      {
        if(line!=null){
          val array = line.split(":");
          if(array.length>0){
            println("array0:"+array{0}+"#")
            println("array1:"+array{1}+"#")
            docs(array{0}.trim()) = array{1}.trim()
            println("array1:"+array{1}.trim()+"#")

          }
        }
      }
    }
    docs

  }
  def read(is: InputStream): mutable.HashMap[String, String] = {
    val docs = scala.collection.mutable.HashMap[String, String]()
    val zipInputStream = new ZipInputStream(is)
    var curZipEntry = zipInputStream.getNextEntry
    while (curZipEntry != null) {
      if (!curZipEntry.isDirectory) {
        val zipEntryName = curZipEntry.getName
        val sb = new StringBuilder()
        val scanner = new Scanner(zipInputStream, "UTF-8")
        while (scanner.hasNextLine) {
          val line =  scanner.nextLine()
          if(line!=null){
            val array = line.split(":");
            if(array.length>0){
              println(array.toString)
              docs(array{0}) = array{1}
            }
          }
        }
      }
      curZipEntry = zipInputStream.getNextEntry
    }
    docs
  }

}
