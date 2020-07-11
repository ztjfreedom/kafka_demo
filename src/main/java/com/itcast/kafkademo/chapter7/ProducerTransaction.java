package com.itcast.kafkademo.chapter7;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Producer事务的使用
 */
public class ProducerTransaction {
    public static final String topic = "heima";
    public static final String brokerList = "localhost:9092";
    public static final String transactionId = "transactionId";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        // properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  幂等性，默认即为true

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            // 处理业务逻辑并创建ProducerRecord
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "msg1");
            producer.send(record1);

            // 模拟事务回滚案例
            System.out.println(1/0);

            ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "msg2");
            producer.send(record2);
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, "msg3");
            producer.send(record3);

            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
        }
        producer.close();
    }

}