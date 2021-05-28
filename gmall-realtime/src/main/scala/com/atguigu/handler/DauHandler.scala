package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 批次内去重
    * @param filterByMidDStream
    * @return
    */
  def filterByGroup(filterByMidDStream: DStream[StartUpLog]) = {
    //1.将数据转为KV形式，K->当天的时间+mid v->value
    val dateWithMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByMidDStream.map(log => {
      ((log.logDate, log.mid), log)
    })

    //2.使用groupBykey算子将相同key的数据聚和到一块
    val dateWitMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateWithMidToLogDStream.groupByKey()

    //3.对value的数据按照时间戳排序，并取第一条
    val value: DStream[((String, String), List[StartUpLog])] = dateWitMidToLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    //4.将value中list集合里面每个样例类压平
    value.flatMap(_._2)
  }

  /**
    * 跨批次去重
    *
    * @param startUpLogDStream
    */
  def filterByMid(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
//    (方案一)
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(startUp => {
      //1.创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //2.查看当前的mid在redis中是否有数据
      val redisKey: String = "DAU:" + startUp.logDate

      //3.过滤掉存在的数据
      val boolean: lang.Boolean = jedis.sismember(redisKey, startUp.mid)

      //4.关闭连接
      jedis.close()
      !boolean
    })
    value*/

   /* //方案二（优化连接->在每个分区下获取连接，以减少连接个数）
    val value1: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //1.在每个分区下获取连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      //遍历每个分区下的数据
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        //2.查看当前的mid在redis中是否有数据
        val redisKey: String = "DAU:" + log.logDate
        //3.过滤掉存在的数据
        val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)

        !boolean
      })
      //关闭连接
      jedis.close()
      logs
    })
    value1*/

    //方案三：最终优化（在每个批次下获取一次连接）
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(startUpLog => {
      //1.获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //2.查询redis中数据
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将redis中的数据广播到executor端
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.过滤数据
      val filterRDD: RDD[StartUpLog] = startUpLog.filter(log => {
        val bool: Boolean = midsBC.value.contains(log.mid)
        !bool
      })

      //关闭连接
      jedis.close()
      filterRDD
    })
    value3
  }

  /**
    * 将去重后的mid保存至redis
    *
    * @param startUpLogDStream
    */
  def saveAsRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //在分区下创建redis连接，以此减少连接次数
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(log=>{
          //1.redisKey
          val redisKey: String = "DAU:"+log.logDate
          //2.将mid写入redis
          jedis.sadd(redisKey,log.mid)
        })
        jedis.close()
      })
    })

  }


}
