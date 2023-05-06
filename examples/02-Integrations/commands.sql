
-- Start KsqlDB CLI
-- docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

-----------------
-- RATINGS
-----------------

-- Create datagen-ratings connector
CREATE SINK CONNECTOR DATAGEN_RATINGS WITH (
    'connector.class' = 'io.confluent.kafka.connect.datagen.DatagenConnector',
    'key.converter'   = 'org.apache.kafka.connect.storage.StringConverter',
    'kafka.topic'     = 'ratings',
    'max.interval'    = 750,
    'quickstart'      = 'ratings',
    'tasks.max'       = 1
);

-- Create the "ratings" stream with automatic AVRO Schema from the Topic
CREATE STREAM RATINGS 
WITH (KAFKA_TOPIC='ratings',VALUE_FORMAT='AVRO');

-- Create a new Stream `RATINGS_LIVE` that filter non test data. 
CREATE STREAM RATINGS_LIVE AS
SELECT * FROM RATINGS 
WHERE LCASE(CHANNEL) NOT LIKE '%test%'
EMIT CHANGES;

-- Create a new Stream `RATINGS_LIVE` that filter test data
CREATE STREAM RATINGS_TEST AS
SELECT * FROM RATINGS 
WHERE LCASE(CHANNEL) LIKE '%test%' 
EMIT CHANGES;

-- Create the KINK Conectector
CREATE SINK CONNECTOR SINK_ES_RATINGS WITH (
    'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    'topics'          = 'ratings',
    'connection.url'  = 'http://elasticsearch:9200',
    'type.name'       = '_doc',
    'key.ignore'      = 'false',
    'schema.ignore'   = 'true',
    'transforms'= 'ExtractTimestamp',
    'transforms.ExtractTimestamp.type'= 'org.apache.kafka.connect.transforms.InsertField$Value',
    'transforms.ExtractTimestamp.timestamp.field' = 'RATING_TS'
);

-----------------
-- CUSTOMERS
-----------------

-- Create Source for Customers data (Be careful using " instead ', since it's SQL statements and quotes are specific)
CREATE SOURCE CONNECTOR SOURCE_DB_CUSTOMERS WITH (
    'connector.class'             = 'io.debezium.connector.mysql.MySqlConnector',
    'tasks.max'                   = '1',
    'database.hostname'           = 'mysql',
    'database.port'               = '3306',
    'database.user'               = 'debezium',
    'database.password'           = 'dbz',
    'database.server.id'          = '42',
    'topic.prefix'                ='mysql',
    'table.include.list'          = 'demo.customers', 
    'include.schema.changes'      = 'false',
    'transforms'                  = 'unwrap,extractkey',
    'transforms.unwrap.type'      = 'io.debezium.transforms.ExtractNewRecordState',
    'transforms.extractkey.type'  = 'org.apache.kafka.connect.transforms.ExtractField$Key',
    'transforms.extractkey.field' = 'id',
    'key.converter'               = 'org.apache.kafka.connect.storage.StringConverter',
    'value.converter'             = 'io.confluent.connect.avro.AvroConverter',
    'schema.history.internal.kafka.bootstrap.servers' = 'broker:29092',
    'schema.history.internal.kafka.topic'             = 'dbhistory.demo' ,
    'value.converter.schema.registry.url'             = 'http://schema-registry:8081'
    );

-- Create `CUSTOMERS` table from 'mysql.demo.CUSTOMERS' source
CREATE TABLE CUSTOMERS (CUSTOMER_ID VARCHAR PRIMARY KEY)
WITH (KAFKA_TOPIC='mysql.demo.CUSTOMERS', VALUE_FORMAT='AVRO');

-----------------
-- JOIN
-----------------

-- Join "Ratings" Stream and "Customers" table into "RATINGS_WITH_CUSTOMER_DATA" Stream and "ratings-enriched" Topic
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM RATINGS_WITH_CUSTOMER_DATA
       WITH (KAFKA_TOPIC='ratings-enriched')
       AS
SELECT R.RATING_ID, R.MESSAGE, R.STARS, R.CHANNEL,
       C.CUSTOMER_ID, C.FIRST_NAME + ' ' + C.LAST_NAME AS FULL_NAME,
       C.CLUB_STATUS, C.EMAIL
FROM   RATINGS_LIVE R
       LEFT JOIN CUSTOMERS C
         ON CAST(R.USER_ID AS STRING) = C.CUSTOMER_ID
WHERE  C.FIRST_NAME IS NOT NULL
EMIT CHANGES;

-- Create Stream of unhappy VIPs from previous Join
CREATE STREAM UNHAPPY_PLATINUM_CUSTOMERS AS
SELECT FULL_NAME, CLUB_STATUS, EMAIL, STARS, MESSAGE
FROM   RATINGS_WITH_CUSTOMER_DATA
WHERE  STARS < 3
  AND  CLUB_STATUS = 'platinum'
PARTITION BY FULL_NAME;

-- Create the connector to sink previous Topics created into ElasticSearch
CREATE SINK CONNECTOR SINK_ES_CUSTOMERS WITH (
  'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'connection.url' = 'http://elasticsearch:9200',
  'type.name' = '',
  'behavior.on.malformed.documents' = 'warn',
  'errors.tolerance' = 'all',
  'errors.log.enable' = 'true',
  'errors.log.include.messages' = 'true',
  'topics' = 'ratings-enriched,UNHAPPY_PLATINUM_CUSTOMERS',
  'key.ignore' = 'true',
  'schema.ignore' = 'true',
  'key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
  'transforms'= 'ExtractTimestamp',
  'transforms.ExtractTimestamp.type'= 'org.apache.kafka.connect.transforms.InsertField$Value',
  'transforms.ExtractTimestamp.timestamp.field' = 'EXTRACT_TS'
);

-----------------
-- AGGREGATIONS
-----------------

-- Simple aggregation - count of ratings per person, per 15 minutes:
CREATE TABLE RATINGS_PER_CUSTOMER_PER_15MINUTE AS
SELECT FULL_NAME,COUNT(*) AS RATINGS_COUNT, COLLECT_LIST(STARS) AS RATINGS
  FROM RATINGS_WITH_CUSTOMER_DATA
        WINDOW TUMBLING (SIZE 15 MINUTE)
  GROUP BY FULL_NAME
  EMIT CHANGES;

-- Push Query
SELECT TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS WINDOW_START_TS,
       FULL_NAME,
       RATINGS_COUNT,
       RATINGS
  FROM RATINGS_PER_CUSTOMER_PER_15MINUTE
  WHERE FULL_NAME='Rica Blaisdell'
  EMIT CHANGES;

-- Pull Query
SELECT TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS WINDOW_START_TS,
       FULL_NAME,
       RATINGS_COUNT,
       RATINGS
FROM   RATINGS_PER_CUSTOMER_PER_15MINUTE
WHERE  FULL_NAME='Rica Blaisdell'
  AND  WINDOWSTART > '2020-07-06T15:30:00.000';