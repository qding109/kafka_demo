package com.example.kafka_demo;

import com.example.kafka_demo.config.KafkaSendResultHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;


@SpringBootApplication
@RestController
public class KafkaDemoApplication {

    private final Logger logger = LoggerFactory.getLogger(KafkaDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Resource
    private  KafkaSendResultHandler kafkaSendResultHandler;

    @Autowired
    private KafkaTemplate<Object, Object> template;

    @GetMapping("/send/{input}")
    public void sendFoo(@PathVariable String input) {
        //template.setProducerListener(new KafkaSendResultHandler());
        this.template.send("topic_input", input);
    }
    @KafkaListener(id = "webGroup", topics = "topic_input")
    public void listen(String input) {
        logger.info("input value: {}" , input);
    }

    /**
     * 消息转发
     * @param input
     * @return
     */
    @GetMapping("/send1/{input}")
    public void sendFoo1(@PathVariable String input) {
        this.template.send("topic-kl", input);
    }

    @KafkaListener(id = "webGroup1", topics = "topic-kl")
    @SendTo("topic-ckl")
    public String listen1(String input) {
        logger.info("input value: {}", input);
        //进行了消息加工，转发过去加工后的消息

        return input + "hello!";
    }

    @KafkaListener(id = "webGroup2", topics = "topic-ckl")
    public void listen2(String input) {
        logger.info("input value: {}", input);
    }


    /**
     * ReplyingKafkaTemplate是KafkaTemplate的一个子类，除了继承父类的方法，新增了一个方法sendAndReceive，实现了消息发送\回复语义
     * @param containerFactory
     * @return
     */

    @Bean
    public ConcurrentMessageListenerContainer<String, String> repliesContainer(ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
        ConcurrentMessageListenerContainer<String, String> repliesContainer = containerFactory.createContainer("replies");
        repliesContainer.getContainerProperties().setGroupId("repliesGroup");
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingTemplate(ProducerFactory<String, String> pf, ConcurrentMessageListenerContainer<String, String> repliesContainer) {
        return new ReplyingKafkaTemplate(pf, repliesContainer);
    }

    @Bean
    public KafkaTemplate kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate(pf);
    }
    @Resource
    private ReplyingKafkaTemplate replyingKafkaTemplate;

    @GetMapping("/send3/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public void sendFoo2(@PathVariable String input) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic-klk", input);
        RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        System.err.println("Return value: " + consumerRecord.value());
    }

    @KafkaListener(id = "webGroup3", topics = "topic-klk")
    @SendTo
    public String listen3(String input) {
        logger.info("input value: {}", input);
        return "successful";
    }

    /**
     * 事务
     * 在事务中发送两条消息，即使第一条发送成功也会因为第二条发送失败而回滚
     * 注意：要在配置中开启事务
     */
    @GetMapping("/send2/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public void sendFoo3(@PathVariable String input) {
        template.send("topic_input", "kl");
        if ("error".equals(input)) {
            throw new RuntimeException("failed");
        }
        template.send("topic_input", "ckl");
    }

}
