package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;


public class MainApp {
    private static final Logger log = LoggerFactory.getLogger(MainApp.class);
    public static void main(String[] args) {

        String elasticsearchHosts = "localhost:9300";
        String elasticsearchIndex = "my-index-for-kafka-connect";
        String kafkaTopic = "my-kafka-topic";
        String dlqTopic = "my-dlq-topic";
        String bootstrapServers = "localhost:9092";


        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-kafka-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(kafkaTopic));

        Map<String,String> sinkProps = new HashMap<>();
        sinkProps.put(CustomElasticsearchSinkTask.ELASTICSEARCH_HOSTS_CONFIG,elasticsearchHosts);
        sinkProps.put(CustomElasticsearchSinkTask.ELASTICSEARCH_INDEX_CONFIG,elasticsearchIndex);
        sinkProps.put(CustomElasticsearchSinkTask.BATCH_SIZE_CONFIG, "50");
        sinkProps.put(CustomElasticsearchSinkTask.DLQ_TOPIC_CONFIG, dlqTopic);
        sinkProps.put(CustomElasticsearchSinkTask.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        CustomElasticsearchSinkTask sinkTask = new CustomElasticsearchSinkTask();
        sinkTask.start(sinkProps);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    List<SinkRecord> sinkRecords = new ArrayList<>();
                    for (ConsumerRecord<String, String> record : records) {
                        SinkRecord sinkRecord = new SinkRecord(record.topic(), record.partition(), null, 
                        record.key(), null, record.value(), record.offset());
                        sinkRecords.add(sinkRecord);
                    }
                    sinkTask.put(sinkRecords);
                }

            }
        }catch(Exception e){
            log.error("Error processing records",e);
        }finally {
            sinkTask.stop();
            consumer.close();
        }
    }
}