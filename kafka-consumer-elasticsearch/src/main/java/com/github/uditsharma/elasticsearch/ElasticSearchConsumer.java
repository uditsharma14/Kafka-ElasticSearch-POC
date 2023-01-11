package com.github.uditsharma.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {

public static RestHighLevelClient getRestHighLevelClient(){
    String hostname = "uditsharma-comsearch-2064881517.us-east-1.bonsaisearch.net";
    String userName = "h3e19kc53s";
    String password = "ghc4oraly0";

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,password));
    RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname,443,"https")).
            setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
    RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
    return client;

}

public static void main(String []args) throws IOException {
     final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
     RestHighLevelClient client = getRestHighLevelClient();
     String jsonString = "{\"foor\":\"bar\"}";
    IndexRequest indexRequest = new IndexRequest("kafka-twitter","tweets").source(jsonString, XContentType.JSON);
    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
    String id = indexResponse.getId();
    logger.info("id "+id);

}
}
