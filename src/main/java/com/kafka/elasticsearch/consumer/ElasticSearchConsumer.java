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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.elasticsearch.config.ConfigConstants;

public class ElasticSearchConsumer {

	static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

	public static void main(String[] args) throws IOException {
		RestHighLevelClient client = createClient();

		KafkaConsumer<String, String> consumer = createConsumer();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {

				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(),
						XContentType.JSON);
				IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
				String responseId = response.getId();
				logger.info(responseId);

			}
		}

		// client.close();
	}

	public static KafkaConsumer<String, String> createConsumer() {
		// create Properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConstants.bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConfigConstants.elasticSearchGroup);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigConstants.earliestOffset);

		// create consumer
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// subscribe consumer
		consumer.subscribe(Collections.singleton(ConfigConstants.topic));

		return consumer;
	}

	public static RestHighLevelClient createClient() {
		// Credential provider being used becahse is connecting to elasticSearch on
		// cloud, if was running local, wasn't necessary
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
				ConfigConstants.usernameCredential, ConfigConstants.passwordCredential));

		RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(ConfigConstants.hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(clientBuilder);
		return client;
	}

}
