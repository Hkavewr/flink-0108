package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 将去重后的mid保存至redis
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
