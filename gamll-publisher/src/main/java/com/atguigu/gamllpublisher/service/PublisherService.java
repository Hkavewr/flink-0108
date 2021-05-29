package com.atguigu.gamllpublisher.service;

import java.util.Map;

public interface PublisherService {
    //总数抽象方法
    public Integer getDauTotal(String date);

    //分时抽象方法
    public Map<String, Long> getDauHourTotal(String date);
}
