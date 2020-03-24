package com.santu.kafkapublisher;

import com.santu.kafkapublisher.publisher.KafkaCsvProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPublisherApplication.class, args);
        KafkaCsvProducer kafkaProducer = new KafkaCsvProducer();
        kafkaProducer.publishMessages();
    }

}
