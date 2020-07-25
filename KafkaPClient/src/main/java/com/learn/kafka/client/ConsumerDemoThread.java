package com.learn.kafka.client;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class ConsumerDemoThread {
	static Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());
	public static <V> void main(String[] args) {
		SpringApplication.run(ConsumerDemoThread.class, args);
		ConsumerDemoThread consumerdemowiththread = new ConsumerDemoThread();
		consumerdemowiththread.run();
		
	}
	
	private void run() {
		CountDownLatch latch  = new CountDownLatch(1);
		ConsumerRunnable myRunnable = new ConsumerRunnable(latch);
		Thread td = new Thread(myRunnable);
		td.start();
		
		// adding shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.debug("adding shutdown hook");
			myRunnable.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				logger.debug("Application fully existed");
			}
			
		}
		));
		
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got intrupted", e);
		}finally {
			logger.debug("Application is closing");
		}
	}
	
	public class ConsumerRunnable implements Runnable{
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		private CountDownLatch countdownlatch;
		Properties prp = new Properties();
		KafkaConsumer<String, String> consumer = null;
		ConsumerRecord<String, String> record  = null;
		
		
		public ConsumerRunnable(CountDownLatch latch) {
			this.countdownlatch=latch;
			prp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			prp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			prp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			prp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "106");	
			prp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");	
			consumer = new KafkaConsumer<String, String>(prp);
			consumer.subscribe(Collections.singleton("test1"));
		}

		@Override
		public void run() {
			try {
			while(true) {
				ConsumerRecords<String,String> crecods = consumer.poll(Duration.ofMillis(200));
				for (ConsumerRecord crecord : crecods) {
					logger.info("key= "+crecord.key() + ",value ="+crecord.value()+",offshset ="+crecord.offset()+",partition ="+crecord.partition());
				}
			}
			}catch (WakeupException wakeupexception) {
				logger.info("receivied shutdown signal ="+wakeupexception.getMessage());
			}finally {
				consumer.close();
				countdownlatch.countDown();
			}
		}
		
		public void shutdown() {
			consumer.wakeup();
			
		}
		
		
	}

}
