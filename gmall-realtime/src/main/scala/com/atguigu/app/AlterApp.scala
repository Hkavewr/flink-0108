package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlterApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlterApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka数据（事件日志）
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将数据转为样例类,并返回Kv类型的数据，因为下面groupBykey算子要使用
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全字段
        val times: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)
        (eventLog.mid, eventLog)
      })
    })

    //5.开窗（5min）
    val windowDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

    //6.对相同mid的数据聚和
    val midToIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    //7.筛选数据
    val boolToAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建set集合用来存放uid数据
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建set集合用来存放领优惠券涉及商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建list集合用来存放用户行为
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //定义标志位,用来判断是否有浏览商品行为
        var bool: Boolean = true

        //遍历数据
        breakable {
          iter.foreach(log => {
            //添加用户行为
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              //添加领优惠券所涉及商品ID
              itemIds.add(log.itemid)
            }
          })
        }
        //生成疑似预警日志
        (uids.size() >= 3 & bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //8.生成预警日志
    val alertDStream: DStream[CouponAlertInfo] = boolToAlertDStream.filter(_._1).map(_._2)

    alertDStream.print()

    //9.将预警日志写入ES
    alertDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT, list)
      }
      )
    })

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
