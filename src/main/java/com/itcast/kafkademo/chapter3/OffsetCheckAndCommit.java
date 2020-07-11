package com.itcast.kafkademo.chapter3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class OffsetCheckAndCommit {

    private static final String brokerList = "localhost:9092";
    private static final String topic = "heima";
    private static final String groupId = "group.demo";
    private static AtomicBoolean running = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // Kafka消费者找不到消费位置时，指定从什么位置开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // 开启手动提交
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // consumer.subscribe(Collections.singletonList(topic));
        // 只订阅指定的分区，实际使用的不多
        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(tp));

        long lastConsumedOffset = -1;
        while (running.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) break;

            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();  // 最后一个消息的位置

            // 同步提交消费位移
            consumer.commitSync();
        }

        System.out.println("Consumed offset is: " + lastConsumedOffset);
        OffsetAndMetadata metadata = consumer.committed(tp);
        System.out.println("Committed offset is: " + metadata.offset());
        long position = consumer.position(tp);
        System.out.println("The offset of the next record is: " + position);
    }

}
