package com.atguigu.gamllpublisher.service.impl;

import com.atguigu.gamllpublisher.mapper.DauMapper;
import com.atguigu.gamllpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourTotal(String date) {
        //1.获取数据（Mapper层）
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建map集合用来存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list集合获取到每个map,并将数据重组成新的map
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }
}
