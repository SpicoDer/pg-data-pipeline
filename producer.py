from kafka import KafkaProducer
from faker import Faker
import logging
import json
from time import sleep
import os
import colorlog
import argparse

# Set up command-line arguments
parser = argparse.ArgumentParser(description='Kafka Producer')
parser.add_argument('--topic', default='TestTopic', help='Kafka topic')
parser.add_argument('--delay', type=int, default=2,
                    help='Produce delay of reconnecting in seconds')
parser.add_argument('--dev', action='store_true',
                    help='Development environment flag')
args = parser.parse_args()

# Set environment variables based on command-line arguments
os.environ["KAFKA_TOPIC"] = args.topic
os.environ["PRODUCE_DELAY"] = str(args.delay)
os.environ["DEV_ENV"] = str(args.dev)

# Logger configuration
logger = logging.getLogger()
# Set logging level based on --dev flag
logger.setLevel(logging.DEBUG if args.dev else logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)s:%(name)s:%(message)s'))
logger.addHandler(console_handler)


def create_producer():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        logger.info("Kafka Producer connected successfully")
        return producer
    except Exception as ex:
        logger.error(f"Failed to create Kafka producer: {ex}")
        sleep(5)
        return create_producer()


producer = create_producer()
faker_instance = Faker()


def run_producer():
    for _ in range(10):
        faker_data = {
            "first_name": faker_instance.first_name(),
            "city": faker_instance.city(),
            "phone_number": faker_instance.phone_number(),
            "state": faker_instance.state(),
            "id": str(_)
        }

        payload = json.dumps(faker_data).encode('utf-8')
        response = producer.send(os.getenv("KAFKA_TOPIC"), payload)

        print(f'Response: {response}')
        sleep(int(os.getenv("PRODUCE_DELAY")))


if __name__ == "__main__":
    run_producer()
