import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from event_factory import row_to_event

import pandas as pd
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

# Configuration 

BROKER = os.getenv("BROKER", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")
TOPIC = os.getenv("topic", "orders.v1")
CSV_PATH = os.getenv("csv_path", "data/olist_order_items_dataset.csv")
SCHEMA_PATH = "schemas/order_item.v1.avsc"

# Load Schema
schema_str = Path(SCHEMA_PATH).read_text()
# src that connects to the schema registry
schema_registry_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
    })
# Convert dict to bytes
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
string_serializer = StringSerializer('utf_8')

# Create Producer
producer = Producer({
    "bootstrap.servers": BROKER,
})

# Delivery Callback
def deliver_report(err, msg):
    if err:
        print(f"Delivery failed : {err}")
    else:
        print(f"Delivered to {msg.topic()}")

# Read CSV 
for chunk in pd.read_csv(CSV_PATH, chunksize=1000):
    for _, row in chunk.iterrows():
        event = row_to_event(row)
        # Produce event 
        producer.produce(
            topic=TOPIC,
            key=string_serializer(event["order_id"]),
            value=avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=deliver_report,
        )
        producer.poll(0)

producer.flush()
