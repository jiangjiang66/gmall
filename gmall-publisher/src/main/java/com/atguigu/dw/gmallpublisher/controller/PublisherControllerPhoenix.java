package com.atguigu.dw.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.dw.gmallpublisher.service.PublisherService;
import com.atguigu.dw.gmallpublisher.service.PublisherServicePhoenixImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherControllerPhoenix {
    @Autowired
    PublisherServicePhoenixImpl publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date) {
        List<Map> totalList = new ArrayList<>();

        Map dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", publisherService.getDauTotal(date));
        totalList.add(dauMap);

        Map midMap = new HashMap<>();
        midMap.put("id", "new_mid");
        midMap.put("name", "新增设备");
        midMap.put("value", "1000");
        totalList.add(midMap);

        Map orderAmountMap = new HashMap();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        orderAmountMap.put("value", publisherService.getOrderTotalAmount(date));
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map todayDauHourMap = publisherService.getDauHour(date);
            Map yesterdayDauHourMap = publisherService.getDauHour(date2Yesterday(date));

            Map<String, Map> hourMap = new HashMap<>();
            hourMap.put("today", todayDauHourMap);
            hourMap.put("yesterday", yesterdayDauHourMap);
            return JSON.toJSONString(hourMap);
        } else if ("order_amount".equals(id)) {
            Map todayOrderAmountHourMap = publisherService.getOrderHourTotalAmount(date);
            Map yesterdayOrderAmountHourMap = publisherService.getOrderHourTotalAmount(date2Yesterday(date));

            Map<String, Map> hourMap = new HashMap<>();
            hourMap.put("today", todayOrderAmountHourMap);
            hourMap.put("yesterday", yesterdayOrderAmountHourMap);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }
    

    /**
     * 根据传入的日期转换成昨天
     *
     * @param date
     * @return
     */
    public String date2Yesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }
}
