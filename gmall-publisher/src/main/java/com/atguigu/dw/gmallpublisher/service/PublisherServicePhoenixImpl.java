package com.atguigu.dw.gmallpublisher.service;

import com.atguigu.dw.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.commons.collections.map.HashedMap;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/*
使用phoenix读取Hbase数据
 */
@Service  // 必须添加 Service 注解
public class PublisherServicePhoenixImpl implements PublisherService {
    /*自动注入 DauMapper 对象*/
    @Autowired
    DauMapper dauMapper;

    @Override
    public long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHourList = dauMapper.getDauHour(date);

        Map dauHourMap = new HashedMap();
        for (Map map : dauHourList) {
            String hour = (String)map.get("LOGHOUR");
            Long count = (Long) map.get("COUNT");
            dauHourMap.put(hour, count);
        }

        return dauHourMap;
    }

    /**
     * 获取指定日期订单的销售额
     * @param date
     * @return
     */
    @Override
    public Double getOrderTotalAmount(String date){
       return dauMapper.getOrderAmountTotal(date);
    }


    /**
     * 获取指定日期每个小时的销售额
     * @param date
     * @return
     */
    @Override
    public Map getOrderHourTotalAmount(String date){
        List<Map> orderAmountHour = dauMapper.getOrderAmountHour(date);

        Map<String, BigDecimal> orderHourAmountMap = new HashMap<>();
        for (Map map : orderAmountHour) {
            String hour = (String) map.get("CREATE_HOUR");
            BigDecimal amount = (BigDecimal)map.get("SUM");
            orderHourAmountMap.put(hour, amount);
        }

        return orderHourAmountMap;
    }


}
