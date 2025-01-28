FROM confluentinc/cp-kafka-connect:7.8.0

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:5.5.3

USER 1001