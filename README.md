# kafka_lab01
Syncing data from PostgreSQL RDS Table to Elasticsearch in real-time using Kafka and python containers.  

Two python containers :  SOURCE and SINK. 
- Source container will monitor the PostgreSQL database table every 5ms (or configured interval) and if there is any new data (insert, update or delete), it will capture the new data and send to configured kafka topic.
- Sink container will be listening configured kafka topic and will do propoer data operation (insert, update or delete) to elasticsearch based on the received message from kafka topic and its content.

