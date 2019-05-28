package com.mirana.rabbitmq_test.amqp;


import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMqConfig {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * 死信队列 交换机标识符
     */
    public static final String DEAD_LETTER_EXCHANGE_KEY = "x-dead-letter-exchange";
    /**
     * 死信队列交换机绑定键标识符
     */
    public static final String DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";

    /**
     * 注册队列到Spring的bean
     * 如果不注册，提示错误信息如下：
     * org.springframework.amqp.rabbit.listener.BlockingQueueConsumer$DeclarationException: Failed to declare queue(s):[MyRabbitMQQueue_A]
     *
     * @return 队列
     */
    @Bean
    public Queue createRabbitQueue() {
        // 死信队列的消息转发到业务候补交换机上
        Map<String, Object> args = new HashMap<>();
        args.put(DEAD_LETTER_EXCHANGE_KEY, ConstantQueue.EMAIL_Exchange_DLQ);
        args.put(DEAD_LETTER_ROUTING_KEY, ConstantQueue.EMAIL_ROUTINGKEY_DLQ);
        return QueueBuilder
                .durable(ConstantQueue.QUEUE)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .withArguments(args)// 当 requeue=true ,测试 Nack 或者 reject 是否会进入死信队列
                .build();
    }

    @Bean
    public Queue createRabbitQueueDirect() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_DIRECT_TEST)
//                .nonDurable(ConstantQueue.QUEUE)
//                .exclusive()
                .autoDelete()
                .build();
    }

    @Bean
    public Queue createRabbitQueueUserAdd() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_USER_ADD)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    @Bean
    public Queue createRabbitQueueUserDelete() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_USER_DELETE)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    @Bean
    public Queue createRabbitQueueBookAdd() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_BOOK_ADD)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    @Bean
    public Queue createRabbitQueueBookDelete() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_BOOK_DELETE)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    @Bean
    public Queue createRabbitQueueFanout1() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_FANOUT1)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE_FANOUT1) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    @Bean
    public Queue createRabbitQueueFanout2() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_FANOUT2)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE_FANOUT2) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    /**
     * 创建 DirectExchange
     */
    @Bean
    public DirectExchange createDirectExchange() {
        Map<String, Object> headerMap = new HashMap<>();
        DirectExchange directExchange = (DirectExchange) ExchangeBuilder
                .directExchange(ConstantQueue.DirectExchange)
                .durable(true)// 是否持久化
                .autoDelete()// 自动删除，当断开连接的时候，会自动删除当前Exchange
                .withArguments(headerMap)
                .build();
        return directExchange;
    }

    /**
     * 创建 TopicExchange
     */
    @Bean
    public TopicExchange createTopicExchange() {
        Map<String, Object> headerMap = new HashMap<>();
        TopicExchange topicExchange = (TopicExchange) ExchangeBuilder
                .topicExchange(ConstantQueue.TopicExchange)
                .durable(true)// 是否持久化
                .autoDelete()// 自动删除，当断开连接的时候，会自动删除当前Exchange
                .withArguments(headerMap)
                .build();
        return topicExchange;
    }

    /**
     * 创建 FanoutExchange
     */
    @Bean
    public FanoutExchange createFanoutExchange() {
        Map<String, Object> headerMap = new HashMap<>();
        FanoutExchange fanoutExchange = (FanoutExchange) ExchangeBuilder
                .fanoutExchange(ConstantQueue.FanoutExchange)
                .durable(true)// 是否持久化
                .autoDelete()// 自动删除，当断开连接的时候，会自动删除当前Exchange
                .withArguments(headerMap)
                .build();
        return fanoutExchange;
    }

    /**
     * 将队列绑定到交换机上，并设置消息分发的路由键
     */
    @Bean
    public Binding bindingDirect() {
        return BindingBuilder
                .bind(createRabbitQueueDirect())
                .to(createDirectExchange())
                .with(ConstantQueue.ROUTINGKEY_DIRECT_TEST);
    }

    /**
     * ROUTINGKEY_MATCH_ADD：rk.#.add 匹配新增的操作
     *
     * @return
     */
    @Bean
    public Binding bindingTopic_Add_User() {
        return BindingBuilder
                .bind(createRabbitQueueUserAdd())
                .to(createTopicExchange())
                .with(ConstantQueue.ROUTINGKEY_MATCH_ADD);
    }

    @Bean
    public Binding bindingTopic_Add_Book() {
        return BindingBuilder
                .bind(createRabbitQueueBookAdd())
                .to(createTopicExchange())
                .with(ConstantQueue.ROUTINGKEY_MATCH_ADD);
    }

    /**
     * ROUTINGKEY_MATCH_BOOK：rk.book.# 匹配book的操作
     *
     * @return
     */
    @Bean
    public Binding bindingTopic_Book_Add() {
        return BindingBuilder
                .bind(createRabbitQueueBookAdd())
                .to(createTopicExchange())
                .with(ConstantQueue.ROUTINGKEY_MATCH_BOOK);
    }

    @Bean
    public Binding bindingTopic_Book_Delete() {
        return BindingBuilder
                .bind(createRabbitQueueBookDelete())
                .to(createTopicExchange())
                .with(ConstantQueue.ROUTINGKEY_MATCH_BOOK);
    }


    /**
     * 将队列 queue_fanout_1 绑定到广播交换机 MyFanoutExchange
     *
     * @return
     */
    @Bean
    public Binding bindingFanout1() {
        return BindingBuilder
                .bind(createRabbitQueueFanout1())
                .to(createFanoutExchange());
    }

    /**
     * 将队列 queue_fanout_2 绑定到广播交换机 MyFanoutExchange
     *
     * @return
     */
    @Bean
    public Binding bindingFanout2() {
        return BindingBuilder
                .bind(createRabbitQueueFanout2())
                .to(createFanoutExchange());
    }


    /**
     * 创建邮件死信队列
     *
     * @return
     */
    @Bean
    public Queue createEmailDlqQueue() {
        // 死信队列的消息转发到业务候补交换机上
        Map<String, Object> args = new HashMap<>();
        args.put(DEAD_LETTER_EXCHANGE_KEY, ConstantQueue.EMAIL_Exchange_BZ);
        args.put(DEAD_LETTER_ROUTING_KEY, ConstantQueue.EMAIL_ROUTINGKEY_BZ);
        // 设置死信队列默认 5 秒过期
        args.put("x-message-ttl", 5 * 1000);

        return QueueBuilder
                .durable(ConstantQueue.QUEUE_EMAIL_DLQ)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE_EMAIL_DLQ) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
//                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .withArguments(args)
                .build();
    }

    /**
     * 创建邮件候补队列
     *
     * @return
     */
    @Bean
    public Queue createEmailBzQueue() {

        return QueueBuilder
                .durable(ConstantQueue.QUEUE_EMAIL_BZ)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
//                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }


    /**
     * 创建邮件死信交换机
     *
     * @return
     */
    @Bean
    public DirectExchange createEmailDlqExchange() {
        Map<String, Object> headerMap = new HashMap<>();
        DirectExchange directExchange = (DirectExchange) ExchangeBuilder
                .directExchange(ConstantQueue.EMAIL_Exchange_DLQ)
                .durable(true)// 是否持久化
//                .autoDelete()// 自动删除，当断开连接的时候，会自动删除当前Exchange
                .withArguments(headerMap)
                .build();
        return directExchange;
    }

    /**
     * 创建邮件候补交换机
     *
     * @return
     */
    @Bean
    public DirectExchange createEmailBzExchange() {
        Map<String, Object> headerMap = new HashMap<>();
        DirectExchange directExchange = (DirectExchange) ExchangeBuilder
                .directExchange(ConstantQueue.EMAIL_Exchange_BZ)
                .durable(true)// 是否持久化
//                .autoDelete()// 自动删除，当断开连接的时候，会自动删除当前Exchange
                .withArguments(headerMap)
                .build();
        return directExchange;
    }

    /**
     * 邮件死信队列与死信交换机绑定
     */
    @Bean
    public Binding bindingEmailDqlExchange() {
        return BindingBuilder
                .bind(createEmailDlqQueue())
                .to(createEmailDlqExchange())
                .with(ConstantQueue.EMAIL_ROUTINGKEY_DLQ);
    }

    /**
     * 死信队列与死信交换机绑定
     */
    @Bean
    public Binding bindingEmailBzExchange() {
        return BindingBuilder
                .bind(createEmailBzQueue())
                .to(createEmailBzExchange())
                .with(ConstantQueue.EMAIL_ROUTINGKEY_BZ);
    }

    /**
     * 创建延迟队列
     *
     * @return
     */
    @Bean
    public Queue createDelayedQueue() {
        return QueueBuilder
                .durable(ConstantQueue.QUEUE_DELAYED)// 持久化队列
//                .nonDurable(ConstantQueue.QUEUE) // 非持久化队列，如果断开链接，队列中的数据会丢失
//                .exclusive() // 排他队列，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
                .autoDelete() // 断开链接，自动删除队列，数据也会丢失
                .build();
    }

    /**
     * 创建延时交换机
     *
     * @return
     */
    @Bean
    public CustomExchange createDelayedExchange() {
//        Map<String, Object> args = new HashMap<>();
//        args.put("x-delayed-type", "direct");
//        DirectExchange directExchange = new DirectExchange(ConstantQueue.EXCHANGE_DELAYED, true, false, args);
//        directExchange.setDelayed(true);
//        return directExchange;
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        // 需要安装插件 rabbitmq_delayed_message_exchange
        // 否则报错：Channel shutdown: connection error; protocol method: #method<connection.close>(reply-code=503, reply-text=COMMAND_INVALID - invalid exchange type 'x-delayed-message', class-id=40, method-id=10)
        return new CustomExchange(ConstantQueue.EXCHANGE_DELAYED, "x-delayed-message", true, false, args);
    }


    /**
     * 延迟队列与延迟交换机绑定
     */
    @Bean
    public Binding bindingDelayed() {
        return BindingBuilder
                .bind(createDelayedQueue())
                .to(createDelayedExchange())
                .with(ConstantQueue.ROUTINGKEY_DELAYED)
                .noargs();
    }
}