package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class CustomElasticsearchSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CustomElasticsearchSinkTask.class);

    public static final String ELASTICSEARCH_HOSTS_CONFIG = "elasticsearch.hosts"; // Duplicated from SinkConnector
    public static final String ELASTICSEARCH_INDEX_CONFIG = "elasticsearch.index"; // Duplicated from SinkConnector

    private static final String BATCH_SIZE_CONFIG = "elasticsearch.batch.size";
    private static final String DLQ_TOPIC_CONFIG = "dlq.topic";
    private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    private static final int DEFAULT_BATCH_SIZE = 100;

    private String elasticsearchHosts;
    private String elasticsearchIndex;
    private String elasticsearchType;
    private String dlqTopic;
    private String bootstrapServers;
    private int batchSize;
    private TransportClient client;
    private List<IndexRequest> bulkRequests;
    private KafkaProducer<String, String> dlqProducer;

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        elasticsearchHosts = Objects.requireNonNull(props.get(ELASTICSEARCH_HOSTS_CONFIG), "Elasticsearch Hosts cannot be null");
        elasticsearchIndex = Objects.requireNonNull(props.get(ELASTICSEARCH_INDEX_CONFIG), "Elasticsearch Index cannot be null");
        elasticsearchType = props.getOrDefault(CustomElasticsearchSinkConnector.ELASTICSEARCH_TYPE_CONFIG,"_doc");
        batchSize = Integer.parseInt(props.getOrDefault(BATCH_SIZE_CONFIG, String.valueOf(DEFAULT_BATCH_SIZE)));
        dlqTopic = props.get(DLQ_TOPIC_CONFIG);
        bootstrapServers = Objects.requireNonNull(props.get(BOOTSTRAP_SERVERS_CONFIG), "Bootstrap Servers cannot be null");
        bulkRequests = new ArrayList<>();
        try {
            client = createTransportClient();
        } catch (Exception e) {
            log.error("Error creating ElasticSearch Client", e);
        }
        if (dlqTopic != null) {
            dlqProducer = createDlqProducer();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (client == null) {
            log.warn("Elasticsearch client not initialized. Skipping records.");
            return;
        }
        for (SinkRecord record : records) {
            try {
                if (record.value() != null) {
                    String jsonString = record.value().toString();
                    String indexName = getIndexName(jsonString);
                    IndexRequest request = new IndexRequest(indexName, elasticsearchType)
                            .source(jsonString, XContentType.JSON);
                    bulkRequests.add(request);

                    if (bulkRequests.size() >= batchSize) {
                        flush();
                    }
                } else {
                    log.warn("Record value is null. Skipping indexing for this record");
                }
            }catch (Exception e) {
                log.error("Error indexing record: {}, Reason: {}", record.value().toString(), ExceptionUtils.getStackTrace(e));
                sendToDlq(record,e);
            }
        }
        flush(); //flush any remaining requests at the end of the put operation
    }

    @Override
    public void stop() {
        if(dlqProducer!=null){
            dlqProducer.close();
            log.info("DLQ Kafka producer has been closed");
        }
        if(client!=null){
            client.close();
            log.info("Elasticsearch client has been closed");
        }
    }
    //using transport client
    private TransportClient createTransportClient() throws UnknownHostException{
        Settings settings = Settings.builder()
                .put("client.transport.sniff", true)
                .build();

        PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);

        String[] hosts = elasticsearchHosts.split(",");
        for (String host : hosts) {
            String[] parts = host.split(":");
            String hostname = parts[0].trim();
            int port = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 9300;
            preBuiltTransportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(hostname), port));
        }

        return preBuiltTransportClient;
    }
    private String getIndexName(String jsonString){
        String indexName = elasticsearchIndex;
        try {
            JsonNode jsonNode = mapper.readTree(jsonString);
            if(jsonNode.has("ProductType")){
                String productType = jsonNode.get("ProductType").asText();
                indexName = elasticsearchIndex + "-" + productType.toLowerCase();

            }else {
                log.warn("The message does not have ProductType field: {}. Using default index name {}",jsonString, indexName);
            }
        }catch(Exception e){
            log.warn("Error parsing json: {}. Using default index name {}",jsonString, indexName,e);
        }
        return indexName;
    }
    private void flush() {
        if (!bulkRequests.isEmpty()) {
            try {
                BulkRequest bulkRequest = new BulkRequest();
                 for (IndexRequest request : bulkRequests) {
                   bulkRequest.add(request);
                 }
                BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
                if (bulkResponse.hasFailures()) {
                    log.error("Error occurred during bulk indexing. Details {}", bulkResponse.buildFailureMessage());
                }else{
                    log.info("Indexed {} documents in bulk", bulkRequests.size());
                }
            } catch (Exception e) {
                log.error("Error during bulk index, reason {}", ExceptionUtils.getStackTrace(e));
                bulkRequests.forEach(request -> sendToDlq(request, e));
            } finally {
                bulkRequests.clear();
            }
        }
    }
    private KafkaProducer<String, String> createDlqProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void sendToDlq(SinkRecord record, Exception e) {
        if(dlqProducer==null){
            log.warn("DLQ Producer was not configured. Discarding failed record {}",record);
            return;
        }
        try {
            String errorMessage = String.format("Error indexing record: %s, Reason: %s", record.value(), ExceptionUtils.getStackTrace(e));
            ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, errorMessage);
            dlqProducer.send(dlqRecord);
            log.warn("Sent failed record to DLQ topic {}. Record: {}", dlqTopic, record);
        } catch (Exception ex) {
            log.error("Error sending record to DLQ: {}", ex.getMessage(),ex);
        }
    }
    private void sendToDlq(IndexRequest request, Exception e) {
        if(dlqProducer==null){
            log.warn("DLQ Producer was not configured. Discarding failed record {}",request);
            return;
        }
        try {
            String errorMessage = String.format("Error indexing record: %s, Reason: %s", request.source(), ExceptionUtils.getStackTrace(e));
            ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, errorMessage);
            dlqProducer.send(dlqRecord);
            log.warn("Sent failed record to DLQ topic {}. Record: {}", dlqTopic, request.source());
        } catch (Exception ex) {
            log.error("Error sending record to DLQ: {}", ex.getMessage(),ex);
        }
    }
}