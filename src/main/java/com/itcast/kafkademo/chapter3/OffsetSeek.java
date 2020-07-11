package com.itcast.kafkademo.chapter3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class OffsetSeek {

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
        consumer.subscribe(Collections.singletonList(topic));

        consumer.poll(Duration.ofMillis(2000));
        Set<TopicPartition> assignment = consumer.assignment();  // 获取当前消费者的分区
        System.out.println(assignment);

        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 3);  // partition表示分区，offset表示从哪个位置开始消费
        }

        // 指定从分区末尾开始消费
        // Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        // for (TopicPartition tp : assignment) {
        //     consumer.seek(tp, offsets.get(tp) + 1);
        // }

        while (running.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + " : " + record.value());
            }
        }

    }

}
