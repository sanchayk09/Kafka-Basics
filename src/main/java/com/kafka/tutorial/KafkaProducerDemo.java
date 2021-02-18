package com.kafka.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {

	public static void main(String args[]) {

		//config kafka producer
		String bootstrapServer= "127.0.0.1:9092";
		Properties prop=new Properties();
		/*
		 * prop.setProperty("bootstrap.server",bootstrapServer);
		 * prop.setProperty("key.serializer","");
		 * prop.setProperty("value.serializer","");
		 */
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		//Create kafka producer
		
		KafkaProducer<String,String> producer=new KafkaProducer<String,String>(prop);

		//create a produerRecord which needs to be send
		ProducerRecord<String, String> record=
				new ProducerRecord<String, String>("first_topic","JAVA_KEY" ,"Java with key module");
		//send data async needs to flush or close to see record on client in sync
		producer.send(record);
		producer.close();


	}
}
