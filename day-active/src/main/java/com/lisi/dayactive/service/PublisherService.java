package com.lisi.dayactive.service;

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
}
