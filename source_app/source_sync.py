import psycopg2
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("source_app.log"),
        logging.StreamHandler()
    ]
)

# loading config
with open("./config.json",) as f:
    config = json.load(f)

# postgresql database Connection details
host = config["dbhost"]
database = config["db"]
user = config["dbuser"]
password = config["dbpassword"]
table = config["log_table"]


# Kafka producer configuration settings
kafka_server = config["kfservers"]
topic_name = config["kftopic"]

while True:
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        logging.info("Database connection successful.")

        # Create a cursor object
        cur = conn.cursor()

        # # Get the initial row count
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        last_row_count = cur.fetchone()[0]


        # Set up Kafka producer
        producer = None
        attempts = 0
        while producer is None and attempts < 3:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=kafka_server,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks = "all" # ack 0 : don't wait ack, 1: wait for leader ack, all(-1) : wait for available replica ack.
                    )
            except KafkaError as e:
                attempts += 1
                logging.error("Error connecting to Kafka: %s", str(e))
                if attempts < 3:
                    time.sleep(3)

        if producer is None:
            logging.error("Failed to connect to Kafka after 3 attempts")
            exit(1)
        else:
            logging.info("Kafka connect successful.")


        while True:

            # Get the current time as a timestamp
            timestamp = time.time()

            # Convert the timestamp to a string with the desired format
            date_string = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

            # print out to stdout for user checkin
            logging.info("{} - Running. Last scanned row ID of log table : {}".format(date_string,last_row_count))

            # Get the current row count
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            row_count = cur.fetchone()[0]

            if row_count > last_row_count:
                # Get the new rows
                cur.execute(f"SELECT * FROM {table} OFFSET {last_row_count};")

                # fetchone() get the next row of resultset of previous query. 
                # for example selet query return 3 rows, then it result would be (01,02,03) but when fetchone() is used its will
                # be ([01],[02],[03],none). Each fetchone() will get one after another and last one will be 'none'.
                # That's why "while" is used to get result of next result and stop when it reach to 'none'.

                row = cur.fetchone()
                while row is not None:
                    logging.info("New row detected.")
                    # print(row)

                    # convert datatime obj from log_table into epoch timestamp
                    dt_obj = row[3]
                    # convert the datetime object to epoch timestamp
                    epoch_timestamp = int(dt_obj.timestamp())

                    # convert result tuple into dict to inject into kafka
                    message = {
                        'operation': row[0],
                        'table_name': row[1],
                        'timestamp': epoch_timestamp
                    }

                    message['row_data'] = row[2]

                    # sent to kafka topic
                    metadata = producer.send(topic_name, value=message).get(timeout=10)

                    logging.info("Msg sent to kafka.\nMsg : {}\nMsg offset : {}".format(message, metadata.offset))

                    # fetching next row in result set
                    row = cur.fetchone()

                # fetchall() will load entire result set on the memory. If the results are too many, it can exhaust the memroy.

                # new_rows = cur.fetchall()
                # print("new rows : {}".format(new_rows))
                # print("New row(s) added:")
                # for row in new_rows:
                #     print(row)

            # Update the last row count
            last_row_count = row_count

            # Wait for 5 seconds before checking again
            time.sleep(int(config["sync_duration"]))
            
    except (Exception, psycopg2.DatabaseError, KafkaError) as e:
        # Log the error
        logging.error("Error occurred: %s", str(e))

        # Close the cursor and connection
        cur.close()
        conn.close()

        # Exit the script
        break
