#!/bin/bash

# Start ElasticSearch
/usr/local/bin/docker-entrypoint.sh &

echo "Waiting for Elasticsearch to start ⏳"
#curl --retry 10 -f --retry-all-errors --retry-delay 5 -s -o /dev/null "http://localhost:9200/"
until [ "$(curl -s -w '%{http_code}' -o /dev/null "http://localhost:9200/")" -eq 200 ]; do
  sleep 5
done

# Create the "kafkaconnect" Index
curl -s -XPUT "http://localhost:9200/_index_template/kafkaconnect" -H 'Content-Type: application/json' -d'
    {
      "index_patterns": [ "ratings*" ],
      "template": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": { "dynamic_templates": [ { "dates": { "match": "*_TS", "mapping": { "type": "date" } } } ] }
      }
    }'

sleep infinity