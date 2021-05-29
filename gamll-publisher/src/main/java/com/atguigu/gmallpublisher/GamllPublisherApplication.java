package com.atguigu.gmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gamllpublisher.mapper")
public class GamllPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GamllPublisherApplication.class, args);
    }

}
