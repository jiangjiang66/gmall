package com.lisi.dayactive.service;

import com.lisi.dayactive.mapper.DauMapper;
import org.apache.commons.collections.map.HashedMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
@Service
public class PublisherServiceImpl implements PublisherService{
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
}
