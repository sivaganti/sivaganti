package com.learn.kafka.client;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class ConsumerDemoGroups {
	static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
	public static <V> void main(String[] args) {
		SpringApplication.run(ConsumerDemoGroups.class, args);
		// create producer properties
		Properties prp = new Properties();
		prp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		prp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "107");	
		prp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");	
		//create producer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prp);
		ConsumerRecord<String, String> record  = null;
		consumer.subscribe(Collections.singleton("test1"));
		
		while(true) {
			ConsumerRecords<String,String> crecods = consumer.poll(Duration.ofMillis(200));
			for (ConsumerRecord crecord : crecods) {
				logger.info("key= "+crecord.key() + ",value ="+crecord.value()+",offshset ="+crecord.offset()+",partition ="+crecord.partition());
			}
		}
		
	}

}
