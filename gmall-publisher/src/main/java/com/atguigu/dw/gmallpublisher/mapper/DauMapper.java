package com.atguigu.dw.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/*
从数据库查询数据的接口
 */
public interface DauMapper {
    // 查询日活总数
    long getDauTotal(String date);
    // 查询小时明细
    List<Map> getDauHour(String date);
    /**
     * 获取订单总的销售额
     *
     * @param date
     * @return
     */
    double getOrderAmountTotal(String date);

    /**
     * 获取每小时的销售额明细
     *
     * @param date
     * @return
     */
    List<Map> getOrderAmountHour(String date);
}
