package com.mirana.rabbitmq_test.amqp;

import com.mirana.rabbitmq_test.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AmqpController {

    @Autowired
    private MqSender sender;


    @GetMapping("send2Queue")
    public String send2Queue(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String nowDate = DateUtils.format();
        sender.send2Queue(nowDate, msgNumber);
        return "成功发送 " + msgNumber + " 条消息： " + nowDate;
    }

    @GetMapping("send2DirectExchange")
    public String send2DirectExchange(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String nowDate = DateUtils.format();
        sender.send2DirectExchange(nowDate, msgNumber);
        return "成功发送 " + msgNumber + " 条消息： " + nowDate;
    }

    @GetMapping("send2TopicExchange")
    public String send2TopicExchange(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String nowDate = DateUtils.format();
        sender.send2TopicExchange(nowDate, msgNumber);
        return "成功发送 " + msgNumber + " 条消息： " + nowDate;
    }

    @GetMapping("send2FanoutExchange")
    public String send2FanoutExchange(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String nowDate = DateUtils.format();
        sender.send2FanoutExchange(nowDate, msgNumber);
        return "成功发送 " + msgNumber + " 条消息： " + nowDate;
    }

    @GetMapping("send2EmailDlqQueue")
    public String send2EmailDlqQueue(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String nowDate = DateUtils.format();
        sender.send2EmailDlqQueue(nowDate, msgNumber);
        return "成功发送到死信队列 " + msgNumber + " 条消息： " + nowDate;
    }
}
