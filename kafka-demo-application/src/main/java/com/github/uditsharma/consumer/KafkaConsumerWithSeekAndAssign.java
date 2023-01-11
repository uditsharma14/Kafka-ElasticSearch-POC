package com.github.uditsharma.consumer;

import com.github.uditsharma.producer.KafkaProducerAppWithCallBack;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerWithSeekAndAssign {

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


        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        TopicPartition topicPartition = new TopicPartition(topic,0);
        long offsetToread =15L;
        int numberOfMessagesToRead =5;
        int numberOfMessagesReadSoFar =0;
        boolean keepOnReading = true;
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        kafkaConsumer.seek(topicPartition,offsetToread);
        while(keepOnReading){

            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
                numberOfMessagesReadSoFar++;
             logger.info("Kafka read from TOPIC "+consumerRecord.topic()+ "and Partition "+consumerRecord.partition() + "and Offest " + consumerRecord.offset());
             logger.info(" Message key "+consumerRecord.key()+ " Value "+consumerRecord.value().toString()+ "and Partition"+consumerRecord.partition());
             if(numberOfMessagesToRead<numberOfMessagesReadSoFar){
                 keepOnReading = false;
                 break;
             }
            }

        }
    }
}
