# import json

# from kafka import KafkaConsumer, KafkaProducer

# kafka_server = ["192.168.1.22"]

# topic = "test_topic"

# consumer = KafkaConsumer(
#     bootstrap_servers = kafka_server,
#     value_deserializer = json.loads,
#     auto_offset_reset="latest",
#     api_version=(2, 0, 2)
# )

# consumer.subscribe(topic)

# while True:
#     data = next(consumer)
#     print(data)
#     print(data.value)

import json
from kafka import KafkaConsumer, KafkaProducer

kafka_server = ["192.168.1.22"]

topic = "test_topic"

consumer = KafkaConsumer(
    bootstrap_servers = kafka_server,
    value_deserializer = json.loads,
    auto_offset_reset = "latest",
    api_version = (2, 0, 2),
)

consumer.subscribe(topic)

while True:
    data = next(consumer)
    print(data)
    print(data.value)