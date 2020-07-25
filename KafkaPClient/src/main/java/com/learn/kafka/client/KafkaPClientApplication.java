package com.learn.kafka.client;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class KafkaPClientApplication {

	public static <V> void main(String[] args) {
		SpringApplication.run(KafkaPClientApplication.class, args);
		System.out.println("in main");
		// create producer properties
		Properties prp = new Properties();
		prp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		prp.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		prp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			
		
		//create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prp);
		ProducerRecord<String, String> record  = null;
		// producer record
		for(int i =0; i<50;i++) {
	    try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		record  = new ProducerRecord<String, String> ("test1","Siva "+i);
		System.out.println(i);
		// send data
		
		producer.send(record);
		producer.flush();
		}
		producer.close();
		
	}

}
