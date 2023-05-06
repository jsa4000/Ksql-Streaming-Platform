#!/bin/bash

echo "Installing connector plugins"
confluent-hub install --no-prompt debezium/debezium-connector-mysql:2.1.4
confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.6.0
confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:14.0.6
#
# -----------
# Launch the Kafka Connect worker
/etc/confluent/docker/run &

#echo "Waiting for Kafka Connect to start listening on localhost ⏳"
#while : ; do
#  curl_status=$$(curl -s -o /dev/null -w %{http_code} http://localhost:8089/connectors)
#  echo -e $$(date) " Kafka Connect listener HTTP state: " $$curl_status " (waiting for 200)"
#  if [ $$curl_status -eq 200 ] ; then
#    break
#  fi
#  sleep 5 
#done
#
#echo "Creating Data Generator source"
#curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/datagen-pageviews/config \
#    -d '{
#    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
#    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
#    "kafka.topic": "ratings",
#    "max.interval":750,
#    "quickstart": "ratings",
#    "tasks.max": 1
#}'

sleep infinity