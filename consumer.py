from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import argparse
import colorlog
import os
import logging
from time import sleep
import requests
import json

# Set up command-line arguments
parser = argparse.ArgumentParser(description='Kafka Consumer')
parser.add_argument('--topic', default='TestTopic', help='Kafka topic')
parser.add_argument('--dev', action='store_true',
                    help='Development environment flag')
args = parser.parse_args()

# # Set environment variables based on command-line arguments
os.environ["DEV_ENV"] = str(args.dev)
os.environ["KAFKA_TOPIC"] = args.topic
os.environ["ES_ENDPOINT"] = "http://localhost:9200"

# Logger configuration
logger = logging.getLogger()
# Set logging level based on --dev flag
logger.setLevel(logging.DEBUG if args.dev else logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)s:%(name)s:%(message)s'))
logger.addHandler(console_handler)


class ElasticSearchUpload:

    def __init__(self, json_data, hash_key, index_name):
        self.json_data = json_data
        self.hash_key = hash_key
        self.index_name = index_name.lower()

    def upload(self):
        url = f"{os.getenv('ES_ENDPOINT')}/{self.index_name}/_doc/{self.hash_key}"
        print(f"URL: {url}")

        headers = {"Content-Type": "application/json"}
        response = requests.put(url, headers=headers,
                                data=json.dumps(self.json_data))
        print(response)
        return {"status": response.status_code, "data": {"message": "Record uploaded to Elasticsearch"}}


def connect_elasticsearch():
    es = Elasticsearch(
        [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

    if es.ping():
        logger.info("Elasticsearch connected successfully")
        return es
    else:
        logger.error("Failed to connect to Elasticsearch")
        sleep(5)
        return connect_elasticsearch()


def check_index(elasticsearch, index_name):
    if elasticsearch and not elasticsearch.indices.exists(index=index_name):
        try:
            elasticsearch.indices.create(index=index_name)
            print(f"Index '{index_name}' was successfully created.")
        except Exception as e:
            print(f"Failed to create the index '{index_name}': {str(e)}")
    elif elasticsearch:
        print(f"Index '{index_name}' already exists.")


def consume_topics(consumer):
    for msg in consumer:
        payload = json.loads(msg.value)
        payload["meta_data"] = {
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "timestamp": msg.timestamp,
            "timestamp_type": msg.timestamp_type,
            "key": msg.key,
        }

        ElasticSearchUpload(json_data=payload,
                            index_name=payload.get("meta_data").get("topic"),
                            hash_key=payload.get("meta_data").get("offset")).upload()


def run_consumer():
    es = connect_elasticsearch()
    check_index(es, os.getenv("KAFKA_TOPIC").lower())
    consumer = KafkaConsumer(os.getenv("KAFKA_TOPIC"),
                             auto_offset_reset='earliest')
    consume_topics(consumer)


run_consumer()
