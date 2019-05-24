package com.mirana.rabbitmq_test.amqp;


import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Component
public class MqSender {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @PostConstruct
    public void rabbitTemplateInit() {
        // 使用jackson 消息转换器
//        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
//        rabbitTemplate.setEncoding("UTF-8");
        // 当mandatory标志位设置为true时，如果exchange根据自身类型和消息routingKey无法找到一个合适的queue存储消息，那么broker会调用basic.return方法将消息返还给生产者;
        // 当mandatory设置为false时，出现上述情况broker会直接将消息丢弃;通俗的讲，mandatory标志告诉broker代理服务器至少将消息route到一个队列中，否则就将消息return给发送者;
//        rabbitTemplate.setMandatory(true);

        // 开启returncallback yml 需要 配置 publisher-returns: true
        // 实现ReturnCallback
        // 当消息发送出去找不到对应路由队列时，将会把消息退回
        // 如果有任何一个路由队列接收投递消息成功，则不会退回消息
        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            log.info("消息被退回: {}, {} {} {} {}", new String(message.getBody()), replyCode, replyText, exchange, routingKey);
        });

        // 消息确认 yml 需要配置  publisher-returns: true
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                log.info("消息发送成功,id: {}", correlationData == null ? "" : correlationData.getId());
            } else {
                log.info("消息发送到失败,原因: {}", cause);
            }
        });
    }


    public void send2Queue(String msg, int msgNumber) {

        log.info("send2Queue: {}", msg);
        // 向 MyRabbitMQQueue 发送消息
        for (int i = 0; i < msgNumber; i++) {
            String sendMsg = i + "_" + msg;
            // 发送持久化消息，rabbitmq 重启后消息依然在队列里
            Message<String> message = MessageBuilder
                    .withPayload(sendMsg)
                    .setHeader(MessageHeaders.CONTENT_TYPE, MessageProperties.CONTENT_TYPE_JSON)
                    .build();

            CorrelationData correlationData = new CorrelationData();
            correlationData.setId(UUID.randomUUID().toString());
            correlationData.setReturnedMessage(new org.springframework.amqp.core.Message(("this is return msg").getBytes(), null));

            this.rabbitTemplate.convertAndSend(ConstantQueue.QUEUE, message, correlationData);
        }
    }

    public void send2DirectExchange(String msg, int msgNumber) {

        log.info("send2DirectExchange: {}", msg);

        for (int i = 0; i < msgNumber; i++) {
            this.rabbitTemplate.convertAndSend(ConstantQueue.DirectExchange, ConstantQueue.ROUTINGKEY_DIRECT_TEST, i + "_" + msg);
        }
    }

    public void send2TopicExchange(String msg, int msgNumber) {

        log.info("send2TopicExchange: {}", msg);


//        String ROUTINGKEY_MATCH_BOOK = "rk.book.#";
//        String ROUTINGKEY_MATCH_ADD = "rk.#.add";

        int ackMode = new Random().nextInt(100) % 4;

        for (int i = 0; i < msgNumber; i++) {
            if (ackMode == 0) {
                // 匹配routingkey ROUTINGKEY_MATCH_BOOK，当前路由键监听的队列有：queue_book.add、queue_book.delete
                this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_BOOK_DELETE, i + "_" + msg);
            } else if (ackMode == 1) {
                // 匹配routingkey ROUTINGKEY_MATCH_BOOK、ROUTINGKEY_MATCH_ADD，当前路由键监听的队列有：queue_book.add、queue_book.delete、queue_user.add
                this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_BOOK_ADD, i + "_" + msg);
            } else if (ackMode == 2) {
                // 匹配routingkey ROUTINGKEY_MATCH_ADD，当前路由键监听的队列有 queue_book.add、queue_user.add
                this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_USER_ADD, i + "_" + msg);
            } else if (ackMode == 3) {
                // ROUTINGKEY_MATCH_ADD 和 ROUTINGKEY_MATCH_BOOK 两个路由键都不匹配
                this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_USER_DELETE, i + "_" + msg);
            } else {
                // 无法匹配路由键 @returnedMessage异常：NO_ROUTE MyTopicExchange rk.other
                this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_OTHER, i + "_" + msg);
            }
        }
    }

    public void send2FanoutExchange(String msg, int msgNumber) {

        log.info("send2FanoutExchange: {}", msg);

        for (int i = 0; i < msgNumber; i++) {
            this.rabbitTemplate.convertAndSend(ConstantQueue.FanoutExchange, "", i + "_" + msg);
        }
    }

    public void send2EmailDlqQueue(String msg, int msgNumber) {
        log.info("send2EmailDlqQueue: {}", msg);

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
//        声明消息处理器  这个对消息进行处理  可以设置一些参数   对消息进行一些定制化处理   我们这里  来设置消息的编码  以及消息的过期时间  因为在.net 以及其他版本过期时间不一致   这里的时间毫秒值 为字符串
        MessagePostProcessor messagePostProcessor = message -> {
            MessageProperties messageProperties = message.getMessageProperties();
//            设置编码
            messageProperties.setContentEncoding("utf-8");
//            设置过期时间10*1000毫秒
            messageProperties.setExpiration("10000");
            // 消息持久化
            messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            // 毫秒为单位，指定此消息的延时时长
            messageProperties.setDelay(5 * 1000);
            return message;
        };

        for (int i = 0; i < msgNumber; i++) {
            this.rabbitTemplate.convertAndSend(ConstantQueue.EMAIL_Exchange_DLQ, ConstantQueue.EMAIL_ROUTINGKEY_DLQ, i + "_" + msg, messagePostProcessor, correlationData);
        }
    }
}
