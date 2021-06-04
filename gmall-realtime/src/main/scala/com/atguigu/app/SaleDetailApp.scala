package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._

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
    orderIdToOptDStream.mapPartitions(partition=>{
      //TODO 创建结果集合用来存放能够管理上的数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建 Redis连接
      val jedis: Jedis = new Jedis("hadoop102",6379)
      partition.foreach{case(orderId,(infoOpt,detailOpt))=>
          //a1.判断orderInfo有没有数据
        if (infoOpt.isDefined){
          //a1.1有orderInfo数据
          val orderInfo: OrderInfo = infoOpt.get
          //a1.2判断是有有能够关联上的orderDetail数据
          if (detailOpt.isDefined){
            //获取orderDetail的数据
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail: SaleDetail = new SaleDetail(orderInfo,orderDetail)
            details.add(saleDetail)
          }
          //a2.将orderInfo数据写入redis缓存，并设置过期时间（为了等晚来的OrderDetail数据）
          val infoRedisKey: String = "orderInfo"+orderId
          //将样例类转为String类型
          import org.json4s.native.Serialization
          implicit val formats=org.json4s.DefaultFormats
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(infoRedisKey,orderInfoJson)
          //设置过期时间
          jedis.expire(infoRedisKey,10)

          //a3.查询缓存中是否有对应的orderDetail
          //a3.1查询是否有对应的Key
          //设置orderDetail的redisKey
          val detailRedisKey: String = "orderDetail"+orderId
          if (jedis.exists(detailRedisKey)){
            //此时有能够关联上的detail数据，因此获取到detail数据
            val orderDetailSet: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetailSet.asScala) {
              //将查询出来的orderDetail数据（String类型），转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem,classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo,orderDetail)
              details.add(detail)
            }
          }




        }


      }
      partition
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
