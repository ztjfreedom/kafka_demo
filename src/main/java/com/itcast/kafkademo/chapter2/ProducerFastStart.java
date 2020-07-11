package com.itcast.kafkademo.chapter2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerFastStart {

    private static final String brokerList = "localhost:9092";
    private static final String topic = "heima";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key序列化器
        // properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        // 设置值序列化器
        // properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置集群地址
        // properties.put("bootstrap.servers", brokerList);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "kafka-demo", "Hello Java Kafka");
        // ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 10 * 1000, "kafka-demo", "Hello Java Kafka");

        try {
            // 同步发送，通过返回值可以对消息发送状态进行确认
            // Future<RecordMetadata> send = producer.send(record);
            // RecordMetadata recordMetadata = send.get();
            // System.out.println("topic: " + recordMetadata.topic());
            // System.out.println("partition: " + recordMetadata.partition());
            // System.out.println("offset: " + recordMetadata.offset());

            // 异步发送，通过回调函数获得返回值
            producer.send(record, (recMetadata, exception) -> {
                if (exception == null) {
                    System.out.println("topic: " + recMetadata.topic());
                    System.out.println("partition: " + recMetadata.partition());
                    System.out.println("offset: " + recMetadata.offset());
                }
            });
            System.out.println("展示异步发送");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
