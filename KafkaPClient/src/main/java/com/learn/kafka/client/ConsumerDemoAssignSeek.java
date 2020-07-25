package com.learn.kafka.client;

import java.time.Duration;
import java.util.Arrays;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class ConsumerDemoAssignSeek {
	static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
	public static <V> void main(String[] args) {
		SpringApplication.run(ConsumerDemoAssignSeek.class, args);
		// create producer properties
		Properties prp = new Properties();
		prp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		prp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	//	prp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "106");	
		prp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");	
		
	//create producer
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prp);
		
		// Assign
		TopicPartition partitiontoreadfrom = new TopicPartition("test1", 0);
		consumer.assign(Arrays.asList(partitiontoreadfrom));
		
		// seek
		long offsetstoreadfrom =15;
		consumer.seek(partitiontoreadfrom, offsetstoreadfrom);
	
		ConsumerRecord<String, String> record  = null;
	//	consumer.subscribe(Collections.singleton("test1"));
		
		int numberofmessagetoread = 5;
		boolean keeponreading= true;
		int numberofmessagesreadsofar =0;
		
		while(keeponreading) {
			ConsumerRecords<String,String> crecods = consumer.poll(Duration.ofMillis(200));
			for (ConsumerRecord crecord : crecods) {
				numberofmessagesreadsofar++;
				logger.info("key= "+crecord.key() + ",value ="+crecord.value()+",offshset ="+crecord.offset()+",partition ="+crecord.partition()+" no of messages read ="+numberofmessagesreadsofar);
				if (numberofmessagesreadsofar > numberofmessagetoread) {
					keeponreading=false;
					break;
				}
			}
			
		}
		logger.debug("exiting the application");;
		
	}

}
