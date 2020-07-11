package com.itcast.kafkademo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@RequestMapping
public class KafkaDemoApplication {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @RequestMapping("/index")
    public String index() {
        return "Hello Kafka";
    }

    @Autowired
    private KafkaTemplate template;

    private static final String topic = "heima";

    @GetMapping("/send/{input}")
    public String sendToKafka(@PathVariable String input) {
        // 发送消息
        // this.template.send(topic, input);

        // 事务
        this.template.executeInTransaction(t -> {
            t.send(topic, input);
            if ("error".equals(input)) {
                throw new RuntimeException("Input is error");
            }
            t.send(topic, "Second message");
            return true;
        });

        return "Send success: " + input;
    }

    // 注解的方式实现事务控制
    @GetMapping("/sendtran/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public String sendToKafkaTran(@PathVariable String input) {
        this.template.send(topic, input);
        if ("error".equals(input)) {
            throw new RuntimeException("Input is error");
        }
        this.template.send(topic, "Second message");
        return "Send success: " + input;
    }

    @KafkaListener(id="", topics = topic, groupId = "group.demo")
    public void listener(String input) {
        LOGGER.info("Message input value: {}", input);
    }

}
