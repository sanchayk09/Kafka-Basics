package com.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String args[]) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        //config kafka producer
        String bootstrapServer = "127.0.0.1:9092";
        Properties prop = new Properties();
        /*
         * prop.setProperty("bootstrap.server",bootstrapServer);
         * prop.setProperty("key.serializer","");
         * prop.setProperty("value.serializer","");
         */
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create kafka producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        for (int i = 0; i < 10; i++) {
            //create a produerRecord which needs to be send
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello World From Java "+i);
            //send data async needs to flush or close to see record on client in sync
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received new Metada:\n"
                                + "Topic: " + metadata.topic() + "\n"
                                + "Offset: " + metadata.offset() + "\n"
                                + "TimeStamp: " + metadata.timestamp()
                        );
                    } else {
                        logger.error("Error while Producing: " + exception);
                    }


                }
            });

            System.out.println("Hello World");

        }producer.close();
    }
}
