package com.example.kafka_demo.config;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * 一个回调类，处理生产者发送消息后的回调
 * 不知道为什么不管用
 * @author qding
 */
@Component
public class KafkaSendResultHandler implements ProducerListener {

    //注意这里的logger引入的包  slf4j
    private static final Logger log = LoggerFactory.getLogger(KafkaSendResultHandler.class);

    @Override
    public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata) {
        log.info("注意1：",topic+" "+value);
    }

    @Override
    public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
        log.info("注意2：",producerRecord.toString());
    }
}
