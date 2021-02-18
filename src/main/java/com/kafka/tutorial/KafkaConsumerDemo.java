package com.kafka.tutorial;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.naming.NameClassPair;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConsumerDemo {

	public static void main(String[] args) {
		Logger logger=LoggerFactory.getLogger(KafkaConsumerDemo.class);
		
		String bootStrapServer="127.0.0.1:9092";
		String groupID="my-fifthx-application";
		String topic="first_topic";
		
		Properties prop=new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer );
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName() );
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName() );
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		
		KafkaConsumer<String, String> consumer=
				new KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList(topic));
		//System.out.println(consumer.listTopics().toString());
		while(true)
		{
			ConsumerRecords<String,String> records=
					consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String,String> record:records) {System.out.println("asas");
				System.out.println("key:\t"+record.key()+"\nValue:\t"+record.value());}
		}
		
		

	}

}
