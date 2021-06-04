package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import collection.JavaConversions._
import org.json4s.native.Serialization
object SaleDetailApp {
  def main(args: Array[String]): Unit = {

    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka数据
    //3.1获取订单表数据
    val orderInfoKafKaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    //3.2获取订单详情表数据
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //4.将JSON数据转为样例类
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafKaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        val create_time: String = orderInfo.create_time
        orderInfo.create_date = create_time.split(" ")(0)
        orderInfo.create_hour = create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //5.双流join
//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
//    value.print()
    val orderIdToOptDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //6.通过加缓存的方式结果因网络延迟所带来的的数据丢失问题
    val noUserDStream: DStream[SaleDetail] = orderIdToOptDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //TODO 创建结果集合用来存放能够管理上的数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建 Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>
        //设置orderInfo的redisKey
        val infoRedisKey: String = "orderInfo" + orderId
        //设置orderDetail的redisKey
        val detailRedisKey: String = "orderDetail" + orderId

        //a1.判断orderInfo有没有数据
        if (infoOpt.isDefined) {
          //a1.1有orderInfo数据
          val orderInfo: OrderInfo = infoOpt.get
          //a1.2判断是有有能够关联上的orderDetail数据
          if (detailOpt.isDefined) {
            //获取orderDetail的数据
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(saleDetail)
          }
          //a2.将orderInfo数据写入redis缓存，并设置过期时间（为了等晚来的OrderDetail数据）
          //将样例类转为String类型
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(infoRedisKey, orderInfoJson)
          //设置过期时间
          jedis.expire(infoRedisKey, 10)

          //a3.查询缓存中是否有对应的orderDetail
          //a3.1查询是否有对应的Key

          if (jedis.exists(detailRedisKey)) {
            //此时有能够关联上的detail数据，因此获取到detail数据
            val orderDetailSet: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetailSet.asScala) {
              //将查询出来的orderDetail数据（String类型），转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          //b.orderInfo没有，orderDetail有
          val orderDetail: OrderDetail = detailOpt.get
          //b1.判断对方缓存中（orderInfo）是否有更够关联上的数据
          if (jedis.exists(infoRedisKey)) {
            //有能够关联上的orderInfo数据,并取出缓存的orderInfo数据
            val infoStr: String = jedis.get(infoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(infoStr, classOf[OrderInfo])
            val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(detail)
          } else {
            //b2.对方缓存中没有能关联上的数据，则将自己存入缓存等待后面批次的orderInfo数据来关联
            //将样例类转为json字符串(String)
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedis.sadd(detailRedisKey, orderDetailJson)
            //设置过期时间
            jedis.expire(detailRedisKey, 10)
          }
        }
      }
      jedis.close()
      details.toIterator
    })

    //7.查询userInfo缓存，补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(partition => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val saleDetails: Iterator[SaleDetail] = partition.map(saleDetail => {
        //去userInfo缓存中获取数据
        val userRedisKey: String = "userInfo" + saleDetail.user_id
        val userStr: String = jedis.get(userRedisKey)
        //将查询出来的String类型的userInfo转成样例类，为了方便调用SaleDetail样例类中的方法补全用户信息用的
        val userInfo: UserInfo = JSON.parseObject(userStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      saleDetails
    })
    saleDetailDStream.print()

    //8.将SaleDetail数据写入ES
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_SALEDETAIL+System.currentTimeMillis()/1000/60/60/24,list)
      })
    })
//    //打印测试订单表数据
//    val orderInfoDStream: DStream[String] = orderInfoKafKaDStream.map(record=>record.value())
//    orderInfoDStream.print()

//    //打印测试订单详情数据
//    val orderDetailDStream: DStream[String] = orderDetailKafkaDStream.map(record=>record.value())
//    orderDetailDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
