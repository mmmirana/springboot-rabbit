package com.mirana.rabbitmq_test.amqp;


import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

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


    public void send2Queue(Message message, CorrelationData correlationData) {
        this.rabbitTemplate.convertAndSend(ConstantQueue.QUEUE, message, correlationData);
    }

    public void send2DirectExchange(String msg) {
        this.rabbitTemplate.convertAndSend(ConstantQueue.DirectExchange, ConstantQueue.ROUTINGKEY_DIRECT_TEST, msg);
    }

    public void send2TopicExchange(String msg, Integer routingkeyMode) {

//        String ROUTINGKEY_MATCH_BOOK = "rk.book.#";
//        String ROUTINGKEY_MATCH_ADD = "rk.#.add";

        if (routingkeyMode == 0) {
            // 匹配routingkey ROUTINGKEY_MATCH_BOOK，当前路由键监听的队列有：queue_book.add、queue_book.delete
            this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_BOOK_DELETE, msg);
        } else if (routingkeyMode == 1) {
            // 匹配routingkey ROUTINGKEY_MATCH_BOOK、ROUTINGKEY_MATCH_ADD，当前路由键监听的队列有：queue_book.add、queue_book.delete、queue_user.add
            this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_BOOK_ADD, msg);
        } else if (routingkeyMode == 2) {
            // 匹配routingkey ROUTINGKEY_MATCH_ADD，当前路由键监听的队列有 queue_book.add、queue_user.add
            this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_USER_ADD, msg);
        } else if (routingkeyMode == 3) {
            // ROUTINGKEY_MATCH_ADD 和 ROUTINGKEY_MATCH_BOOK 两个路由键都不匹配
            this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_USER_DELETE, msg);
        } else {
            // 无法匹配路由键 @returnedMessage异常：NO_ROUTE MyTopicExchange rk.other
            this.rabbitTemplate.convertAndSend(ConstantQueue.TopicExchange, ConstantQueue.ROUTINGKEY_OTHER, msg);
        }
    }

    public void send2FanoutExchange(String msg) {
        this.rabbitTemplate.convertAndSend(ConstantQueue.FanoutExchange, "", msg);
    }

    public void send2EmailDlqQueue(String msg, CorrelationData correlationData, MessagePostProcessor messagePostProcessor) {
        this.rabbitTemplate.convertAndSend(ConstantQueue.EMAIL_Exchange_DLQ, ConstantQueue.EMAIL_ROUTINGKEY_DLQ, msg, messagePostProcessor, correlationData);
    }

    /**
     * 发送消息到延时队列
     *
     * @param msg                  消息
     * @param correlationData      id
     * @param messagePostProcessor 消息处理器
     */
    public void send2DelayedQueue(String msg, CorrelationData correlationData, MessagePostProcessor messagePostProcessor) {
        this.rabbitTemplate.convertAndSend(ConstantQueue.EXCHANGE_DELAYED, ConstantQueue.ROUTINGKEY_DELAYED, msg, messagePostProcessor, correlationData);
    }
}
