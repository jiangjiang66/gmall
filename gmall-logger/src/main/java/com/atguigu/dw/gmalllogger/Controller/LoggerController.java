package com.atguigu.dw.gmalllogger.Controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wuhui.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class LoggerController {

    @PostMapping("/log")
    public String logger(@RequestParam("log") String log){
        System.out.println(log);

        //加时间戳
        log = addTs(log);
        System.out.println(log);
        //写磁盘
        saveLog(log);

        //写到kafka  先创建kafak生产者 再调生产者send方法
        sendToKafka(log);
        return "666";
    }

    @Autowired
    KafkaTemplate template;

    private void sendToKafka(String log) {
        String topic = Constant.TOPIC_STARTUP ;
        if(log.contains("event")){
            topic= Constant.TOPIC_EVENT;
        }
        template.send(topic,log);
    }

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);
    private void saveLog(String log) {
        logger.info(log);
    }

    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts",System.currentTimeMillis());
        return obj.toJSONString();
    }

}
