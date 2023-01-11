package com.github.uditsharma.consumer;

import com.github.uditsharma.producer.KafkaProducerAppWithCallBack;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerGroupsApp {

    public static void main(String[] args) {
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-app";
        String topic = "udit-second-topic";
        final Logger logger = LoggerFactory.getLogger(KafkaProducerAppWithCallBack.class);
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        while(true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
             logger.info("Kafka read from TOPIC "+consumerRecord.topic()+ "and Partition "+consumerRecord.partition());
             logger.info(" Message key "+consumerRecord.key()+ " Value "+consumerRecord.value().toString()+ "and Partition"+consumerRecord.partition());
            }
        }
    }
}
