package com.itcast.kafkademo.chapter2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerSerializer {

    private static final String brokerList = "localhost:9092";
    private static final String topic = "heima";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());  // CompanySerializer
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("kafka").address("北京").build();  // 这里需要lombok插件
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
        try {
            // 异步发送，通过回调函数获得返回值
            producer.send(record, (recMetadata, exception) -> {
                if (exception == null) {
                    System.out.println("topic: " + recMetadata.topic());
                    System.out.println("partition: " + recMetadata.partition());
                    System.out.println("offset: " + recMetadata.offset());
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
