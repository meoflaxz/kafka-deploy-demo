# import json

# import json
# from kafka import KafkaConsumer, KafkaProducer

# # kafka_server = ["192.168.1.22"]

# topic = "test_topic"

# consumer = KafkaConsumer(
#     bootstrap_servers = "192.168.1.22:9092",
#     value_deserializer = json.loads,
#     auto_offset_reset = "latest",
#     api_version = (2, 0, 2),
# )

# consumer.subscribe(topic)

# while True:
#     data = next(consumer)
#     print(data)
#     print(data.value)

import json 
from kafka import KafkaConsumer
import psycopg2
from sqlalchemy import create_engine

if __name__ == '__main__':
    conn = psycopg2.connect(
                    host="localhost",
                    database="postgres",
                    user="robert",
                    password="Admin123"
                )

    # Create a cursor object
    cur = conn.cursor()
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'mec-xdr',
        bootstrap_servers='localhost:9092',
        max_poll_records = 100,
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        auto_offset_reset='earliest'#,'smallest'
)
    for message in consumer:
        print(json.loads(message.value))
        cur.execute("INSERT INTO app_data.raw_data (raw) VALUES (%s)", (str(json.loads(message.value)),))
        conn.commit()