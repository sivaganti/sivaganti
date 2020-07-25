package com.learn.kafka.client;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class KafkaPClientApplicationKeys {
	static Logger logger = LoggerFactory.getLogger(KafkaPClientApplicationKeys.class);

	public static <V> void main(String[] args) {
		SpringApplication.run(KafkaPClientApplicationKeys.class, args);
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
		String key="ipshc1";
		for(int i =0; i<50;i++) {
	    try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    if ((i % 2) ==0)
	    	key="ipshc2";
	    else
	    	key="ipshc1";
	    
		record  = new ProducerRecord<String, String> ("test1",key,"Siva "+i);
		// send data
		logger.info("key {}",key);
		producer.send(record,new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if (exception == null) {
					
					logger.info ("offsets {} ", metadata.hasOffset());
					logger.info("topic {} " ,metadata.topic());
					logger.info("partition {}",metadata.partition());
				}else 
					logger.error(exception.getMessage());
				
				
			}
		});
		producer.flush();
		}
		producer.close();
		
	}

}
