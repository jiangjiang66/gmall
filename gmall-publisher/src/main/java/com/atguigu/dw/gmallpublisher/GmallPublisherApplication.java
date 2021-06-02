package com.atguigu.dw.gmallpublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
// 增加扫描包
@MapperScan(basePackages = "com.atguigu.gmall0225publisher2.mapper")
public class GmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisherApplication.class, args);
    }

}
