package com.rf.consumer.api;

import com.rf.consumer.api.dto.User;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@RestController
public class KafkaConsumerApplication {

    List<String> messages = new ArrayList<>();
    User userMessages = null;


    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @GetMapping("/msgFromTopic")
    public List<String> getMessagesFromTopic() {
        return messages;
    }

    @RequestMapping("/userFromTopic")
    public User getUserMessageFromTopic() {
        return userMessages;
    }

    @KafkaListener(groupId = "arif-1", topics = "arif", containerFactory = "kafkaListenerContainerFactory")
    public List<String> getMessagesFromTopic(String data) {
        messages.add(data);
        return messages;
    }


    @KafkaListener(groupId = "arif-2", topics = "arif", containerFactory = "userKafkaListenerContainerFactory")
    public User getUserMessageFromTopic(User user) {
        userMessages = user;
        return userMessages;
    }
}
