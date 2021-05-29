package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;


    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        //1.创建list结合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.从Service层获取日活需求数据
        Integer dauTotal = publisherService.getDauTotal(date);
        //2.1获取交易额总数数据
        Double gmvTotal = publisherService.getGmvTotal(date);

        //3.先将数据封装到map中
        //3.1创建存放新增日活数据的map
        HashMap<String, Object> dauMap = new HashMap<>();
        //3.2创建存放新增设备数据的map
        HashMap<String, Object> devMap = new HashMap<>();
        //3.3创建存放新增交易额数据的map
        HashMap<String, Object> gmvMap = new HashMap<>();

        //3.4将数据放入map集合
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", "233");

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmvTotal);

        //4.将封装完毕的map集合放入list集合中
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date
    ) {
        //1.创建map集合用来存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //2.从Service层获取数据

        String yesterDay = LocalDate.parse(date).plusDays(-1).toString();
        Map todayMap = null;
        Map yesterDayMap = null;
        if ("order_amount".equals(id)) {
            //处理交易额相关的数据
            todayMap = publisherService.getGmvHourTotal(date);

            yesterDayMap = publisherService.getGmvHourTotal(yesterDay);

        } else {
            //日活相关的数据
            //2.1获取今天的数据
            todayMap = publisherService.getDauHourTotal(date);

            //2.2获取昨天的数据
            yesterDayMap = publisherService.getDauHourTotal(yesterDay);
        }

        //3.将数据封装到Map集合中
        result.put("yesterday", yesterDayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }

}
