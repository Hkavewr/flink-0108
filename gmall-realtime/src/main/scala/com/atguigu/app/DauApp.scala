package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.处理数据(将json数据转为样例类并补全logDate和logHour这两个字段)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)
        startUpLog
      })
    })

    //因为后面多次使用，为了提升效率，因为加缓存
//    startUpLogDStream.cache()

    //5.批次间去重
    val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByMid(startUpLogDStream,ssc.sparkContext)
    //打印原始数据个数
//    startUpLogDStream.count().print()

    //经过批次间去重后的数据个数
//    filterByMidDStream.cache()
//    filterByMidDStream.count().print()

    //6.批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByMidDStream)

    //经批次内去重后的数据个数（最终去重的数据个数）
//    filterByGroupDStream.cache()
//    filterByGroupDStream.count().print()

    //7.将去重后的结果保存至redis，以便以下一批数据来的时候做批次间去重
    DauHandler.saveAsRedis(filterByGroupDStream)

    //8.将明细数据写入Hbase
    filterByGroupDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL0108_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })


//    //4.测试kafka数据
//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreach(record=>{
//        println(record.value())
//      })
//    })

    //最后一步：开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
