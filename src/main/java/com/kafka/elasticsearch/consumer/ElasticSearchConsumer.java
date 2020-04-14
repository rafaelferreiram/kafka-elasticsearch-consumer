package com.kafka.elasticsearch.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import com.kafka.elasticsearch.config.ConfigConstants;

public class ElasticSearchConsumer {

	static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

	public static void main(String[] args) throws IOException {
		RestHighLevelClient client = createClient();

		KafkaConsumer<String, String> consumer = createConsumer();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			int recordCounts = records.count();
			logger.info("Received " + recordCounts + " records ");
			BulkRequest bulkRequest = new BulkRequest();

			for (ConsumerRecord<String, String> record : records) {

				try {
					String id = extractIdFromTweet(record.value());

					IndexRequest indexRequest = new IndexRequest().index("twitter").type("tweets").id(id)
							.source(record.value(), XContentType.JSON);

					bulkRequest.add(indexRequest); // adding index request to the bulk
				} catch (NullPointerException e) {
					logger.warn("Skipping bad data :" + record.value());
				}

			}

			if (recordCounts > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Commiting offsets ...");
				consumer.commitSync();
				logger.info("Offsets Commited!");
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		// client.close();
	}

	private static String extractIdFromTweet(String tweetJson) {
		JsonParser jsonParser = new JsonParser();
		return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
	}

	public static KafkaConsumer<String, String> createConsumer() {
		// create Properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConstants.bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConfigConstants.elasticSearchGroup);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigConstants.earliestOffset);
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// subscribe consumer
		consumer.subscribe(Collections.singleton(ConfigConstants.topic));

		return consumer;
	}

	public static RestHighLevelClient createClientLocal() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(HttpHost.create(ConfigConstants.hostnameLocal)));
		return client;
	}

	public static RestHighLevelClient createClient() {
		// Credential provider being used becahse is connecting to elasticSearch on
		// cloud, if was running local, wasn't necessary
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
				ConfigConstants.usernameCredential, ConfigConstants.passwordCredential));

		RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(ConfigConstants.hostnameCloud, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(clientBuilder);
		return client;
	}
}
