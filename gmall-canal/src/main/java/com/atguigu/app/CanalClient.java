package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        //1.创建canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            //2.获取连接
            canalConnector.connect();

            //3.监控指定的数据库
            canalConnector.subscribe("gmall.*");

            //4.获取Message数据
            Message message = canalConnector.get(100);

            //5.获取entry（每个sql执行的结果）
            List<CanalEntry.Entry> entries = message.getEntries();

            //6.判断数据是否有变化
            if (entries.size() <= 0) {
                System.out.println("没有数据，休息一会");
                Thread.sleep(5000);
            } else {
                //有数据，接下来解析数据
                //7.遍历List集合获取每个entry
                for (CanalEntry.Entry entry : entries) {
                    //TODO 8.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //9.获取具体数据
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //10.判断是否为RowData类型，因为这个类型里面才是我们具体的数据
                    if (entryType.equals(CanalEntry.EntryType.ROWDATA)) {
                        //11.获取序列化的数据
                        ByteString storeValue = entry.getStoreValue();
                        //12.对数据进行反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 13.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //TODO 14.获取反序列化后的具体数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }


        }


    }

    /**
     * 根据表名以及事件类型来判断获取哪些数据
     *
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //1.获取订单表中新增的数据
        if (("order_info").equals(tableName) && (CanalEntry.EventType.INSERT).equals(eventType)) {
            //TODO 订单表数据
           saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
        } else if (("order_detail").equals(tableName) && (CanalEntry.EventType.INSERT).equals(eventType)) {
            //TODO 订单明细数据
           saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        } else if (("user_info").equals(tableName) && ((CanalEntry.EventType.INSERT.equals(eventType)) || (CanalEntry.EventType.UPDATE.equals(eventType)))) {
            //TODO 用户表数据
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
            }
        }


    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //获取存放列的集合
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //获取每个列
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //模拟网络延迟，会随机睡0-5秒
//            try {
//                Thread.sleep(new Random().nextInt(5)*1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
