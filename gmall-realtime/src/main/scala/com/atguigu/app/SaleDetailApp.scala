package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    value.print()


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
