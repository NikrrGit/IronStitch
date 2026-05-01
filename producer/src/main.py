import argparse
import os
from uuid import uuid4

from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

# Configuration 

BROKER = os.getenv("BROKER", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")
TOPIC = os.getenv("topic", "orders.v1")
SCV_PATH = os.getenv("schema_path", "schemas/order_item.v1.avsc")

