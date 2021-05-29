package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //日活需求总数抽象方法
    public Integer getDauTotal(String date);

    //日活需求分时抽象方法
    public Map<String, Long> getDauHourTotal(String date);

    //交易额需求总数抽象方法
    public Double getGmvTotal(String date);

    //交易额需求分时抽象方法
    public Map<String, Double> getGmvHourTotal(String date);
}
