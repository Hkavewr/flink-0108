package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka数据
    //3.1获取订单表数据
    val userKafKaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    //4.将userinfo数据转为样例类
    userKafKaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          //将数据转为样例类
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
          //设置redisKey
          val userRedisKey: String = "userInfo"+userInfo.id
          //将数据写入redis
          jedis.set(userRedisKey,record.value())
        })
        jedis.close()
      })
    })

    //打印用户表数据
    userKafKaDStream.map(record=>record.value()).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
