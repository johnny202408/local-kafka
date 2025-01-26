
# https://mvnrepository.com/artifact/org.elasticsearch.client/elasticsearch-rest-high-level-client/6.8.23

curl -X PUT \
     -H "Content-Type: application/json" \
     --data @es-sink-config.json \
     http://localhost:8083/connectors/elasticsearch-sink-connector/config


 #curl http://localhost:8083/connectors/elasticsearch-sink-connector/status

 curl http://localhost:8083/connectors/elasticsearch-sink-connector/status | jq

{
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "my-topic",
    "connection.url": "http://elasticsearch:9200",
    "type.name": "doc",
    "key.ignore": "true",
    "schema.ignore": "true",
    "behavior.on.null.values": "ignore",
    "behavior.on.malformed.documents": "ignore",
    "transforms": "flatten",
    "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
    "transforms.flatten.delimiter": "_",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-topic",
    "errors.deadletterqueue.topic.replication.factor": 1,
    "errors.deadletterqueue.context.headers.enable": true
}


local-dev:
http://localhost:8080
http://localhost:8080/ui/clusters/local/all-topics?perPage=25
http://localhost:9200/_cat/indices?v
http://localhost:9200/my-topic/_search?pretty
http://localhost:8083/connectors/elasticsearch-sink-connector/status


