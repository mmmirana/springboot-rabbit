package com.mirana.rabbitmq_test.amqp;

/**
 * 队列名称
 */
public interface ConstantQueue {
    // queue
    String QUEUE = "queue_test";

    // direct queue
    String QUEUE_DIRECT_TEST = "queue_direct_test";

    // exchange queue
    String QUEUE_USER_ADD = "queue_user.add";
    String QUEUE_USER_DELETE = "queue_user.delete";
    String QUEUE_BOOK_ADD = "queue_book.add";
    String QUEUE_BOOK_DELETE = "queue_book.delete";

    // fanout queue
    String QUEUE_FANOUT1 = "queue_fanout_1";
    String QUEUE_FANOUT2 = "queue_fanout_2";

    // routingKey
    String ROUTINGKEY_USER_ADD = "rk.user.add";
    String ROUTINGKEY_USER_DELETE = "rk.user.delete";
    String ROUTINGKEY_BOOK_ADD = "rk.book.add";
    String ROUTINGKEY_BOOK_DELETE = "rk.book.delete";
    String ROUTINGKEY_OTHER = "rk.other";

    // direct routingKey
    String ROUTINGKEY_DIRECT_TEST = "rk.direct.test";

    // exchange routingKey
    String ROUTINGKEY_MATCH_BOOK = "rk.book.#";
    String ROUTINGKEY_MATCH_ADD = "rk.#.add";

//    // fanout 为群发消息，与路由键没有关系
//    String ROUTINGKEY_FANOUT1 = "rk.fanout1";

    // 直连交换机
    String DirectExchange = "MyDirectExchange";

    // 主题交换机
    String TopicExchange = "MyTopicExchange";

    // 扇形交换机
    String FanoutExchange = "MyFanoutExchange";


    // 死信-邮件队列
    String QUEUE_EMAIL_DLQ = "quque_email_dlq";
    // 死信-邮件交换机direct
    String EMAIL_Exchange_DLQ = "EmailExchangeDlq";
    // 死信-死信路由键
    String EMAIL_ROUTINGKEY_DLQ = "rk.email.dlq";

    // 死信队列=>邮件队列，业务处理的候补队列
    String QUEUE_EMAIL_BZ = "quque_email_bz";
    // 邮件交换机direct
    String EMAIL_Exchange_BZ = "EmailExchangeBz";
    // 邮件路由键
    String EMAIL_ROUTINGKEY_BZ = "rk.email.bz";


}
