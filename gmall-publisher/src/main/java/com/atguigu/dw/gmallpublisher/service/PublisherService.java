package com.atguigu.dw.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    /*
   查询总数
    */
    long getDauTotal(String date);
    /*
    查询小时明细

    相比数据层, 我们把数据结构做下调整, 更方便使用
     */
    Map getDauHour(String date);

    /**
     * 获取指定日期订单的销售额
     * @param day
     * @return
     */
    Double getOrderTotalAmount(String day);

    /**
     * 获取指定日期每个小时的销售额
     * @param day
     * @return
     */
    Map getOrderHourTotalAmount(String day);

    /**
     * 根据需要的聚合字段得到销售明细和聚合结构
     *
     * @param date      要查询的日期
     * @param keyword   要查询关键字
     * @param startPage 开始页面
     * @param size      每页显示多少条记录
     * @param aggField  要聚合的字段
     * @param aggSize   聚合后最多多少条记录
     * @return 1. 总数 2. 聚合结果 3. 明细
     *         {
     *             "total": 100,
     *             "stat" : [
     *                 {
     *                     // 年龄段比例
     *                 },
     *                 {
     *                     // 男女比例
     *                 }
     *             ],
     *             "detail": {
     *                 // 明细
     *             }
     *         }
     */
    Map<String, Object> getSaleDetailAndAggResultByAggField( String date,
                                             String keyword,
                                             int startPage,
                                             int size ,
                                             String aggField,
                                             int aggSize) ;



}
