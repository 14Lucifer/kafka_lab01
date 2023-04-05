import json
from elasticsearch import Elasticsearch, NotFoundError #es 7.13. es 7.14 introduce unsupported error.
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import logging
import time
from datetime import datetime, timezone
import os


# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("sink_app.log"),
        logging.StreamHandler()
    ]
)

# loading config
with open("./config.json",) as f:
    config = json.load(f)

# create Elasticsearch client
es = Elasticsearch(config["esservers"], use_ssl=False)
logging.info("ES coonection successful.")

# Set up Kafka consumer
consumer = None
attempts = 0
while consumer is None and attempts < 5:  # attempting to connect to kafka if fails. 
    try:  
        consumer = KafkaConsumer(
            bootstrap_servers=config["kfservers"],  # Kafka broker(s)
            auto_offset_reset='earliest',  # Start from earliest message
            enable_auto_commit=False,  # Disable auto-commit
            group_id='es-sink-app'  # Consumer group ID
        )
    except KafkaError as e:
        attempts += 1
        logging.error("Error connecting to Kafka: %s", str(e))
        if attempts < 5:
            time.sleep(5)


# Assign the consumer to a specific partition of the topic
partition = int(config["kfpart"])
topic_partition = TopicPartition(config["kftopic"], partition)
consumer.assign([topic_partition])
logging.info("Kafka connection successful.")


# define index name and schema
index_name = config["esindex"]
index_schema = {
    "mappings": {
        "properties": {
            "brand": {"type": "text"},
            "price": {"type": "float"},
            "product": {"type": "text"},
            "category": {"type": "text"},
            "quantity": {"type": "text"},
            "product_id": {"type": "integer"},
            "sub_category": {"type": "text"},
            "timestamp": {"type": "date"}
        }
    }
}

# check if index exists
if not es.indices.exists(index_name):
    # create index with specified schema
    es.indices.create(index_name, body=index_schema)
    logging.info(f"Index '{index_name}' created successfully with schema:\n{index_schema}")
else:
    logging.info(f"Index '{index_name}' already exists")


logging.info("Starting listening.")

# Continuously poll for new messages
for message in consumer:
    
    msg_dict = json.loads(message.value.decode('utf-8'))
    logging.info("New Doc : {}".format(msg_dict["operation"]))

    # generating timestamp
    dt = datetime.now(timezone.utc)
    time_stp = datetime.timestamp(dt)*1000000000
    time_stp_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if msg_dict["operation"]=="insert":

        # adding timestamp for es
        msg_dict["row_data"]["data"]["timestamp"] = time_stp_str

        # insert document into index
        es.index(index=index_name, id=msg_dict["row_data"]["data"]["product_id"] ,body=msg_dict["row_data"]["data"])
        logging.info("Doc inserted successfully")

    elif msg_dict["operation"]=="update":

        # adding timestamp for es
        msg_dict["row_data"]["new_data"]["timestamp"] = time_stp_str

        # insert document into index
        es.update(index=index_name, id=msg_dict["row_data"]["old_data"]["product_id"] ,body={"doc": msg_dict["row_data"]["new_data"]})
        logging.info("Doc updated successfully")

    else:
        # delete document from index (ignore 404 error)
        try:
            es.delete(index=index_name, id=msg_dict["row_data"]["data"]["product_id"])
            logging.info("Doc deleted successfully")
        except NotFoundError:
            logging.info("Doc not found, skipping delete")

    # Manually commit the message offset
    consumer.commit()


