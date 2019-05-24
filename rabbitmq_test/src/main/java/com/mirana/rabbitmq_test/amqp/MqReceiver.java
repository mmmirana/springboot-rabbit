package com.mirana.rabbitmq_test.amqp;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 消息接收器/消费者
 * Exchange和RoutingKey、queue不同时，每条消息都会发送到多个队列，每个队列里面只有一个消费者可以消费
 * Exchange和RoutingKey、queue都相同，每条消息只有一个消费者可以消费
 */
@Slf4j
@Component
public class MqReceiver {

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE)
    public void process(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE, msg.getPayload().toString());

        int ackMode = new Random().nextInt(100) % 4;
        boolean multiple = new Random().nextInt(100) % 2 == 0;
        boolean requeue = new Random().nextInt(100) % 2 == 0;

        //  basicReject：是接收端告诉服务器这个消息我拒绝接收,不处理,可以设置是否放回到队列中还是丢掉，而且只能一次拒绝一个消息
        //  官网中有明确说明不能批量拒绝消息，为解决批量拒绝消息才有了basicNack。

        Map<String, Object> optMap = new HashMap<>();
        optMap.put("ackMode", ackMode);
        optMap.put("multiple", multiple);
        optMap.put("requeue", requeue);
        if (ackMode == 0) {
            log.info("模拟正常消费，opt:{}", optMap);
            // 手工ACK后，说明正常消费，队列会删除消息
            Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
            //  long deliveryTag, boolean multiple
            channel.basicAck(deliveryTag, multiple);
        } else if (ackMode == 1) {
            log.info("模拟消费者不应答，opt:{}", optMap);
            // 不应答，不消费消息，可以多条消息
            Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
            // long deliveryTag, boolean multiple, boolean requeue
            channel.basicNack(deliveryTag, multiple, requeue);
        } else if (ackMode == 2) {
            log.info("模拟消费者拒绝应答，opt:{}", optMap);
            // 拒绝应答，只能针对单条消息
            Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
            //  long deliveryTag, boolean requeue
            channel.basicReject(deliveryTag, requeue);
        } else if (ackMode == 3) {
            log.info("模拟消费者异常，opt:{}", optMap);
            // 模拟异常
            throw new RuntimeException("Receiver Error");
        }
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_DIRECT_TEST)
    public void processDirect(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_DIRECT_TEST, msg.getPayload().toString());

        // 手动确认
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //  long deliveryTag, boolean multiple
        channel.basicAck(deliveryTag, false);
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_USER_ADD)
    public void processA(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_USER_ADD, msg.getPayload().toString());

        // 手动确认
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //  long deliveryTag, boolean multiple
        channel.basicAck(deliveryTag, false);
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_USER_DELETE)
    public void processB(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_USER_DELETE, msg.getPayload().toString());

        // 手动确认
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //  long deliveryTag, boolean multiple
        channel.basicAck(deliveryTag, false);
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_BOOK_ADD)
    public void processC(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_BOOK_ADD, msg.getPayload().toString());

        // 手动确认
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //  long deliveryTag, boolean multiple
        channel.basicAck(deliveryTag, false);
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_BOOK_DELETE)
    public void processD(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_BOOK_DELETE, msg.getPayload().toString());

        // 手动确认
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //  long deliveryTag, boolean multiple
        channel.basicAck(deliveryTag, false);
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_FANOUT1)
    public void processX(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_FANOUT1, msg.getPayload().toString());

        //手工ACK
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        channel.basicAck(deliveryTag, true);
    }

    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_FANOUT2)
    public void processY(Message msg, Channel channel) throws IOException {
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_FANOUT2, msg.getPayload().toString());

        //手工ACK
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        channel.basicAck(deliveryTag, true);
    }


    /**
     * 测试死信队列
     *
     * @param msg
     * @param channel
     * @throws IOException
     */
    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_EMAIL_DLQ)
    public void processEmailDlq(Message msg, Channel channel) throws IOException {

        log.info("{} Receiver : {}", ConstantQueue.QUEUE_EMAIL_DLQ, msg.getPayload().toString());


        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
//        // 手动应答消息
//        channel.basicAck(deliveryTag, true);

//        // 拒绝单条消息，设置为不允许重新进入队列，会进入死信队列
//        channel.basicReject(deliveryTag, false);

        // 拒绝多条信息
//        channel.basicNack(deliveryTag, false, false);
    }


    /**
     * 死信队列=》邮件队列，业务处理的候补队列
     *
     * @param msg
     * @param channel
     * @throws IOException
     */
    @RabbitHandler
    @RabbitListener(queues = ConstantQueue.QUEUE_EMAIL_BZ)
    public void processEmailBz(Message msg, Channel channel) throws IOException {
        // 测试死信队列
        log.info("{} Receiver : {}", ConstantQueue.QUEUE_EMAIL_BZ, msg.getPayload().toString());

        // 应答
        Long deliveryTag = (Long) msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        channel.basicReject(deliveryTag, false);

    }


}
