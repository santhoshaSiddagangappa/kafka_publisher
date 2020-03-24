package com.santu.kafkapublisher.publisher;

import com.santu.kafkapublisher.queue.ProcessingQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Queue;
import java.util.UUID;

public class KafkaPush implements Runnable{

    private String name;

    private String KafkaBrokerEndpoint = "abc";

    private String KafkaTopic = "sample-data";

    private Queue<String> queue = ProcessingQueue.getInstance();

    public KafkaPush(String name){
        this.name = name;
    }

    @Override
    public void run() {
        processData();
    }

    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    private void processData() {

        final Producer<String, String> CsvProducer = ProducerProperties();

        while (queue.size() != 0){
            try {
                String data = queue.poll();
                final ProducerRecord<String, String> csvRecord = new ProducerRecord<>(KafkaTopic, UUID.randomUUID().toString(), data);

                CsvProducer.send(csvRecord, (metadata, exception) -> {
                    if (metadata != null){
                        System.out.println("Message sent successfully");
                    } else {
                        System.out.println("Error occured while sending");
                    }

                });
            } catch (Exception e){
                e.printStackTrace();
            }


        }

    }
}
