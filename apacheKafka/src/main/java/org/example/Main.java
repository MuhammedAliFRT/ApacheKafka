package org.example;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        String topicName="SUBSCRIBER";
        String bootstrapServers="localhost:9092";
        String groupID= "subscriber-group";

        Properties producerproperties=new Properties();
        producerproperties.put("bootstrap.servers",bootstrapServers); //kafkaya bağlanmak için kullanılacak sunucu adresi
        producerproperties.put("key.serializer", IntegerSerializer.class.getName()); //producer tarafından gönderilecek anahtarın seri hizmeti
        producerproperties.put("value.serializer", StringSerializer.class.getName());

        Properties consumerproperties=new Properties();
        consumerproperties.put("bootstrap.servers",bootstrapServers);
        consumerproperties.put("group.id",groupID);
        consumerproperties.put("key.deserializer", IntegerDeserializer.class.getName()); //consumer tarafından alınan anahtar dönüşümü için
		consumerproperties.put("value.deserializer", StringDeserializer.class.getName());

        Producer<Integer,String> producer =new KafkaProducer<>(producerproperties);
        Consumer<Integer,String> consumer =new KafkaConsumer<>(consumerproperties);
        consumer.subscribe(Collections.singleton(topicName));

        Random random = new Random();
        int subscID=0;
        try {
            while(true){
                String subscName="Name: "+random.nextInt(10);
                String subscSurname="Surname: "+random.nextInt(10);
                String msisdn="MSISDN: "+random.nextInt(10);

                String publisherrecord =String.format("%d ,%s ,%s ,%s ",subscID,subscName,subscSurname,msisdn);
                producer.send(new ProducerRecord<>(topicName,publisherrecord));
                subscID++;
                ConsumerRecords<Integer,String> records =consumer.poll(10);
                for (ConsumerRecord<Integer, String> record : records){
                    System.out.println(record.value());
                }
                Thread.sleep(1000);
                if (subscID==10) break;

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
            consumer.close();
        }


    }
}