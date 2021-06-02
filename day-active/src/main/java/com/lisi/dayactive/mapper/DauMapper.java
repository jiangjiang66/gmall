package com.lisi.dayactive.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

@Mapper
public interface DauMapper {
    // 查询日活总数
    long getDauTotal(String date);
    // 查询小时明细
    List<Map> getDauHour(String date);
}
