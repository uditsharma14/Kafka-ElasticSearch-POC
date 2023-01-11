package com.github.uditsharma.twitter;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TweetsApi;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import com.twitter.clientlib.model.Tweet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TwitterJavaProducerApp {


    final Logger logger = LoggerFactory.getLogger(TwitterJavaProducerApp.class);

    public static void main(String[] args) throws ApiException {

        final Logger logger = LoggerFactory.getLogger(TwitterJavaProducerApp.class);
        TwitterJavaProducerApp twitterJavaProducerApp = new TwitterJavaProducerApp();
        List<Tweet> tweetList =  twitterJavaProducerApp.runSearch();

        KafkaProducer kafkaProducer = twitterJavaProducerApp.getKafkaProducer();

        for(Tweet tweet : tweetList){
            ProducerRecord producerRecord = new ProducerRecord("udit-second-topic",tweet.getId(),tweet.toString());
            kafkaProducer.send(producerRecord);
            kafkaProducer.flush();
        }
        kafkaProducer.close();
    }



    public  List<Tweet> runSearch() throws ApiException {
        TwitterCredentialsBearer credentials = new TwitterCredentialsBearer("AAAAAAAAAAAAAAAAAAAAAGNmlAEAAAAA2mR8urK0%2Bk%2BLstk7I06gi3qlusQ%3DleFSenu0TzfwRBlSIsWhDapqA2G0zdIlIqu81WvjecMtTC2nNp");
        TwitterApi apiInstance = new TwitterApi(credentials);
        String query = "Bitcoin";
        int maxResults = 1000;
        List<Tweet> tweets = new ArrayList<>();
        TweetsApi.APItweetsRecentSearchRequest result = apiInstance.tweets().tweetsRecentSearch(query);
        result.maxResults(100);
        for(int count=0;count<=5;count++){
            Get2TweetsSearchRecentResponse response  = result.execute();
            if (response.getData() != null) {
                tweets.addAll(response.getData());
                }
            }
        return tweets;
    }



    public KafkaProducer getKafkaProducer(){
        String bootstrapServer="127.0.0.1:9092";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG ,"snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        return kafkaProducer;
    }
}
