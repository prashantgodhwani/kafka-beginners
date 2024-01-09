package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.event.KeyAdapter;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "https://1zugszcf1k:tdi2j091tt@kafka-course-6419323955.us-east-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //FIRST CREATE AN OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create our Kafka Client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                kafkaConsumer.wakeup();

                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        //we need to create index on opensearch if it doesn't exist
        try(openSearchClient; kafkaConsumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("Wikimedia index already exists.");
            }

            try {
                kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

                //main code logic
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(3000));
                    int recordCount = consumerRecords.count();
                    log.info("Received " + recordCount + " record(s)");

                    BulkRequest bulkRequest = new BulkRequest();

                    for (ConsumerRecord<String, String> record : consumerRecords) {

                        //send record to opensearch

                        //strategy 1
                        //define an ID using Kafka record coordinates
                        //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        //strategy 2
                        //get id from data

                        try {
                            String id = extractId(record.value());

                            IndexRequest indexRequest = new IndexRequest("wikimedia")
                                    .source(record.value(), XContentType.JSON)
                                    .id(id);

//                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

//                        log.info("Inserted 1 document into OpenSearch : " + indexResponse.getId());

                            bulkRequest.add(indexRequest);
                        } catch (Exception e) {
                        }
                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkItemResponses = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        log.info("inserted " + bulkItemResponses.getItems().length + " record(s)");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    //commit offsets after batch is consumed
                    kafkaConsumer.commitSync();
                    log.info("Offsets have been committed");
                }
            } catch (WakeupException var13) {
                log.info("Consumer gracefully shutting down...");
            } catch (Exception var14) {
                log.error("Caught unexpected exception", var14);
            } finally {
                kafkaConsumer.close();
                openSearchClient.close();
                log.info("Offsets committed and consumer has now shut down gracefully!");
            }
        }

        //close things

    }

    private static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject()
                .get("meta").getAsJsonObject()
                .get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "demo_java";
        String groupIdx = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdx);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        return consumer;
    }
}