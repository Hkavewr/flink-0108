package com.atguigu.gamllpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //获取总数日活的数据
    public Integer selectDauTotal(String date);

    //获取分时数据
    public List<Map> selectDauTotalHourMap(String date);

}
