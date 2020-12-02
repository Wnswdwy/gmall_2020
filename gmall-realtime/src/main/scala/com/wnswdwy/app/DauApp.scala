package com.wnswdwy.app

import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.nswdwy.constants.GmallConstant
import com.wnswdwy.DauHandler
import com.wnswdwy.bean.StartUpLog
import com.wnswdwy.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
/**
 * @author yycstart
 * @create 2020-12-01 20:46
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //2. 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //3. 消费Kafka 启动主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP, ssc)
    //4. 将每一行数据转换成样例类对象，并补充时间字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //a.获取value
      val value: String = record.value()
      //b.取出时间戳
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //c. 将时间戳转换成字符串
      val ts: Long = startUpLog.ts
      //d. 给时间字段重新赋值
      val dateStr: String = sdf.format(new Date(ts))
      val dateArr: Array[String] = dateStr.split(" ")
      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)
      //e. 返回数据
      startUpLog
    })

    //原始数据个数打印
    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5.利用Redis做跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    //跨批次去重之后个数打印
    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()

    //6.使用Mid作为Key做同批次去重
    val filterByMid: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)

    //同批次去重之后数据个数打印
    filterByMid.cache()
    filterByMid.count().print()

    //7.将两次去重之后的结果中的Mid写入Redis,给当天后置批次去重使用
    DauHandler.saveMidToRedis(filterByMid)

    //8.将两次去重之后的结果写入HBase(Phoenix)
    filterByMid.foreachRDD(rdd => {

      rdd.saveToPhoenix("GMALL2020_DAU",
//        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })


    //9.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
