import os

from kafka import KafkaProducer
import json
from pathlib import Path

# Get the path to the root directory
root_dir = Path(__file__).resolve().parent.parent

properties_file = root_dir / "client.properties"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open(properties_file) as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, data):
  # creates a new producer instance
  producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                           value_serializer=lambda v: json.dumps(v).encode("utf-8"))

  # produces a sample message
  producer.send(topic, data)

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()