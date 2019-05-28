package com.mirana.rabbitmq_test.amqp;

import com.mirana.rabbitmq_test.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Slf4j
@RestController
public class AmqpController {

    @Autowired
    private MqSender sender;


    /**
     * 发送消息到最简单的队列
     * <p>
     * 20190527 测试 requeue=true, Nack 或者 reject 进入死信队列，最后交给备用邮件业务交换机处理
     * <p>
     * 2019-05-27 15:04:02.533 : 消息发送成功,id: 74eb313b-0478-45ff-9a50-b3158e866b37
     * 2019-05-27 15:16:24.426 : send2Queue: 2019-05-27 15:16:24
     * 2019-05-27 15:16:24.440 : queue_test Receiver : 0_2019-05-27 15:16:24
     * 2019-05-27 15:16:24.440 : 模拟消费者不应答，opt:{}
     * 2019-05-27 15:16:24.508 : 消息发送成功,id: 4e195ba6-8e46-468b-a261-9bde497e4bac
     * 2019-05-27 15:16:29.522 : quque_email_bz Receiver : 0_2019-05-27 15:16:24
     * <p>
     * 2019-05-27 15:16:33.699 : send2Queue: 2019-05-27 15:16:33
     * 2019-05-27 15:16:33.727 : queue_test Receiver : 0_2019-05-27 15:16:33
     * 2019-05-27 15:16:33.727 : 模拟消费者拒绝应答，opt:{}
     * 2019-05-27 15:16:33.762 : 消息发送成功,id: 7cab23f2-bdce-468b-ad31-ea98f885d3cb
     * 2019-05-27 15:16:38.769 : quque_email_bz Receiver : 0_2019-05-27 15:16:33
     *
     * @param msgNumber 消息数量
     * @return 消息发送结果
     */
    @GetMapping("send2Queue")
    public String send2Queue(@RequestParam(required = false, defaultValue = "1") Integer msgNumber,
                             @RequestParam(required = false, defaultValue = "1") Integer ackMode,
                             @RequestParam(required = false, defaultValue = "false") Boolean multiple,
                             @RequestParam(required = false, defaultValue = "true") Boolean requeue) {
        String msg = DateUtils.format();
        log.info("send2Queue: {}", msg);

        Map<String, Object> receiveOptMap = new HashMap<>();
        receiveOptMap.put("ackMode", ackMode);
        receiveOptMap.put("multiple", multiple);
        receiveOptMap.put("requeue", requeue);

        // 向 MyRabbitMQQueue 发送消息
        for (int i = 0; i < msgNumber; i++) {
            String sendMsg = i + "_" + msg;
            // 发送持久化消息，rabbitmq 重启后消息依然在队列里
            Message<String> message = MessageBuilder
                    .withPayload(sendMsg)
                    .setHeader(MessageHeaders.CONTENT_TYPE, MessageProperties.CONTENT_TYPE_JSON)
                    .setHeader("receiveOpt", receiveOptMap)
                    .build();

            CorrelationData correlationData = new CorrelationData();
            correlationData.setId(UUID.randomUUID().toString());

            sender.send2Queue(message, correlationData);
        }

        return "成功发送 " + msgNumber + " 条消息： " + msg;
    }

    /**
     * 发送消息到最 direct交换机
     *
     * @param msgNumber 消息数量
     * @return 消息发送结果
     */
    @GetMapping("send2DirectExchange")
    public String send2DirectExchange(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String msg = DateUtils.format();
        log.info("send2DirectExchange: {}", msg);

        for (int i = 0; i < msgNumber; i++) {
            sender.send2DirectExchange(msg);
        }

        return "成功发送 " + msgNumber + " 条消息： " + msg;
    }

    /**
     * 发送消息到最 topic交换机
     *
     * @param msgNumber      消息数量
     * @param routingkeyMode 路由键模式，不同的路由键匹配的队列不同
     * @return 消息发送结果
     */
    @GetMapping("send2TopicExchange")
    public String send2TopicExchange(@RequestParam(required = false, defaultValue = "1") Integer msgNumber, @RequestParam(required = false, defaultValue = "1") Integer routingkeyMode) {
        String msg = DateUtils.format();
        log.info("send2TopicExchange: {}", msg);

        for (int i = 0; i < msgNumber; i++) {
            sender.send2TopicExchange(msg, routingkeyMode);
        }
        return "成功发送 " + msgNumber + " 条消息： " + msg;
    }

    /**
     * 发送消息到最 fanout交换机-广播模式
     *
     * @param msgNumber 消息数量
     * @return 消息发送结果
     */
    @GetMapping("send2FanoutExchange")
    public String send2FanoutExchange(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String msg = DateUtils.format();
        log.info("send2FanoutExchange: {}", msg);

        for (int i = 0; i < msgNumber; i++) {
            sender.send2FanoutExchange(msg);
        }

        return "成功发送 " + msgNumber + " 条消息： " + msg;
    }

    /**
     * 发送到死信邮件死信交换机
     *
     * @param msgNumber 消息数量
     * @return 消息发送结果
     */
    @GetMapping("send2EmailDlqQueue")
    public String send2EmailDlqQueue(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String msg = DateUtils.format();
        log.info("send2EmailDlqQueue: {}", msg);

        // 2-8 的随机时间，测试死信队列（这里设置5s）的msg存活时间
        int min = 2;
        int max = 8;
        int randomExpiration = (int) (new Random().nextFloat() * (max - min) + min);
        String randomExpirationStr = String.valueOf(randomExpiration * 1000);


//        声明消息处理器  这个对消息进行处理  可以设置一些参数   对消息进行一些定制化处理   我们这里  来设置消息的编码  以及消息的过期时间  因为在.net 以及其他版本过期时间不一致   这里的时间毫秒值 为字符串
        MessagePostProcessor messagePostProcessor = message -> {
            MessageProperties messageProperties = message.getMessageProperties();
//          设置编码
            messageProperties.setContentEncoding("utf-8");
//          在RabbitMQ 3.0.0以后的版本中，TTL 设置可以具体到每一条 message 本身，只要在通过 basic.publish 命令发送 message 时设置 expiration 字段。
//          当消息存活时间小于队列设置值时，以消息存活时间为准；当村小存活时间小于队列设置值时，以队列存活时间为准
//
//          2019-05-27 14:54:58.252  send2EmailDlqQueue: 2019-05-27 14:54:58
//          2019-05-27 14:54:58.464  消息发送成功,id: randomExpiration: 6,e4577d77004543fc83a2b60e209f0671
//          2019-05-27 14:55:03.364  quque_email_bz Receiver : 2019-05-27 14:54:58
//
//          2019-05-27 14:55:05.131  send2EmailDlqQueue: 2019-05-27 14:55:05
//          2019-05-27 14:55:05.169  消息发送成功,id: randomExpiration: 2,2582554077af4d00b3eae99aaced1ad6
//          2019-05-27 14:55:07.144  quque_email_bz Receiver : 2019-05-27 14:55:05
//
//          2019-05-27 14:55:23.660  send2EmailDlqQueue: 2019-05-27 14:55:23
//          2019-05-27 14:55:23.723  消息发送成功,id: randomExpiration: 5,2ce31299f9f046a7a630142f79f618be
//          2019-05-27 14:55:28.670  quque_email_bz Receiver : 2019-05-27 14:55:23
//
//          2019-05-27 14:55:31.496  send2EmailDlqQueue: 2019-05-27 14:55:31
//          2019-05-27 14:55:31.541  消息发送成功,id: randomExpiration: 3,3cb8393cf8a745289f8edb2c7004dff7
//          2019-05-27 14:55:34.522  quque_email_bz Receiver : 2019-05-27 14:55:31
//
//          2019-05-27 14:55:37.775  send2EmailDlqQueue: 2019-05-27 14:55:37
//          2019-05-27 14:55:37.864  消息发送成功,id: randomExpiration: 6,f3e7522b2fd245aaab7d7bd75882ad68
//          2019-05-27 14:55:42.798  quque_email_bz Receiver : 2019-05-27 14:55:37
//
//          2019-05-27 14:55:48.985  send2EmailDlqQueue: 2019-05-27 14:55:48
//          2019-05-27 14:55:49.119  消息发送成功,id: randomExpiration: 4,1fd22879cd8442e1af7116b1379fa1b6
//          2019-05-27 14:55:52.997  quque_email_bz Receiver : 2019-05-27 14:55:48
            messageProperties.setExpiration(randomExpirationStr);
            // 消息持久化
            messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            // 毫秒为单位，指定此消息的延时时长
//            messageProperties.setDelay(5 * 1000);
            return message;
        };

        for (int i = 0; i < msgNumber; i++) {
            CorrelationData correlationData = new CorrelationData("randomExpiration: " + randomExpiration + "," + UUID.randomUUID().toString().replace("-", ""));
            sender.send2EmailDlqQueue(msg, correlationData, messagePostProcessor);
        }

        return "成功发送到死信队列 " + msgNumber + " 条消息： " + msg + ", 延时：" + randomExpiration + " 秒";
    }


    /**
     * 发送消息到延时队列
     * 1、下载插件 rabbitmq_delayed_message_exchange：https://dl.bintray.com/rabbitmq/community-plugins/3.7.x/rabbitmq_delayed_message_exchange/rabbitmq_delayed_message_exchange-20171201-3.7.x.zip
     * <p>
     * 2、解压到rabbitmq_server-3.7.7\plugins目录下,文件: rabbitmq_delayed_message_exchange-20171201-3.7.x.ez
     * <p>
     * 3、rabbitmq安装插件 rabbitmq-plugins enable rabbitmq_delayed_message_exchange
     * <p>
     * 4、关闭rabbitmq服务，net stop rabbitmq
     * <p>
     * 5、删除 C:\Users\${username}\.erlang.cookie、C:\Users\${username}\AppData\Roaming\RabbitMQ\enabled_plugins
     * 然后启用插件：
     * C:\WINDOWS\system32>rabbitmq-plugins enable rabbitmq_delayed_message_exchange
     * Enabling plugins on node rabbit@mirana:
     * rabbitmq_delayed_message_exchange
     * The following plugins have been configured:
     * rabbitmq_delayed_message_exchange
     * Applying plugin configuration to rabbit@mirana...
     * The following plugins have been enabled:
     * rabbitmq_delayed_message_exchange
     * <p>
     * set 1 plugins.
     * Offline change; changes will take effect at broker restart.
     * <p>
     * 6、启动Rabbitmq服务，net start rabbitmq
     * <p>
     * 7、声明延时队列和延时交换机CustomExchange
     * <p>
     * 8、启动应用并访问地址发送延迟消息
     * curl http://localhost:8080/send2DelayedQueue
     * % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
     * Dload  Upload   Total   Spent    Left  Speed
     * 100    62  100    62    0     0    984      0 --:--:-- --:--:-- --:--:--   984
     * ==> 成功发送到延迟队列 1 条消息： 2019-05-28 11:05:48
     * <p>
     * 2019-05-28 11:05:48.037 : send2DelayedQueue: 2019-05-28 11:05:48
     * 2019-05-28 11:05:48.090 : 消息被退回: 2019-05-28 11:05:48, 312 NO_ROUTE exchange_delayed routingkey_delayed
     * 2019-05-28 11:05:48.092 : 消息发送成功,id: af8cc6b7c8ea488eb8e21468ca7202bd
     * 2019-05-28 11:05:53.142 : queue_delayed Receiver : 2019-05-28 11:05:48
     *
     * @param msgNumber 消息数量
     * @return 消息发送结果
     * <p>
     * <p>
     * <p>
     * TODO 这里的消息会被退回
     */
    @GetMapping("send2DelayedQueue")
    public String send2DelayedQueue(@RequestParam(required = false, defaultValue = "1") Integer msgNumber) {
        String msg = DateUtils.format();
        log.info("send2DelayedQueue: {}", msg);

        MessagePostProcessor messagePostProcessor = message -> {
            MessageProperties messageProperties = message.getMessageProperties();
            messageProperties.setContentEncoding("utf-8");
            // 消息持久化
            messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            // 毫秒为单位，指定此消息的延时时长
            messageProperties.setDelay(5 * 1000);
            return message;
        };

        for (int i = 0; i < msgNumber; i++) {
            CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString().replace("-", ""));
            sender.send2DelayedQueue(msg, correlationData, messagePostProcessor);
        }

        return "成功发送到延迟队列 " + msgNumber + " 条消息： " + msg;
    }
}
