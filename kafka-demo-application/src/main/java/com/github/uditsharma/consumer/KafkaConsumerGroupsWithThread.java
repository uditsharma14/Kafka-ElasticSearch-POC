package com.github.uditsharma.consumer;

import com.github.uditsharma.producer.KafkaProducerAppWithCallBack;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerGroupsWithThread {

    public static void main(String[] args) {
       new KafkaConsumerGroupsWithThread().run();
    }

    public void run(){
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-fifth-app";
        String topic = "udit-second-topic";

        final Logger logger = LoggerFactory.getLogger(KafkaProducerAppWithCallBack.class);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating a consumer Thread");
        ConsumerThread consumerThread = new ConsumerThread(countDownLatch,properties,topic);
        Thread thread = new Thread(consumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook");
            consumerThread.shutdown();
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    logger.error("application has interrupted");
                }
            logger.info("application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.info("Application is Interrupted");
        }finally {
            logger.info("application is Closing");
        }
    }





    public class ConsumerThread implements Runnable{

        CountDownLatch countDownLatch;

        String topic;

        KafkaConsumer<String ,String > kafkaConsumer;

        final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);


        public ConsumerThread(CountDownLatch countDownLatch,Properties properties, String topic){
            this.countDownLatch = countDownLatch;
            this.kafkaConsumer= new KafkaConsumer(properties);
            this.topic = topic;
        }
        @Override
        public void run() {
            kafkaConsumer.subscribe(Arrays.asList(topic));
            try{
                while(true){
                    ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
                        logger.info("Kafka read from TOPIC "+consumerRecord.topic()+ "and Partition "+consumerRecord.partition());
                        logger.info(" Message key "+consumerRecord.key()+ " Value "+consumerRecord.value().toString()+ "and Partition"+consumerRecord.partition());
                    }
                }
            }catch (WakeupException wakeupException){
                logger.info("Recieved shutdown signal !");
            }finally {
                kafkaConsumer.close();
                countDownLatch.countDown();
            }


        }

        public void shutdown(){
            kafkaConsumer.wakeup();
        }
    }
}
