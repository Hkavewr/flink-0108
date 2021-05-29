package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //1.获取总数数据
    public Double selectOrderAmountTotal(String date);

    //2.获取分时数据
    public List<Map> selectOrderAmountHourMap(String date);
}
