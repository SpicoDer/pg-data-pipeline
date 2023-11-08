from kafka import KafkaProducer
from faker import Faker
import logging
import json
from time import sleep
import os
import colorlog

# ENV Variables
os.environ["KAFKA_TOPIC"] = "TestTopic"
os.environ["PRODUCE_DELAY"] = "2"
os.environ["DEV_ENV"] = "True"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
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
        logger.error(f"Failed to create kafka producer: {ex}")
        sleep(5)
        create_producer()


producer = create_producer()
faker_instance = Faker()


def run_producer():
    try:
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

            logger.info(f"Producer response: {response}")
            sleep(int(os.getenv("PRODUCE_DELAY")))
    except Exception as ex:
        logger.error(f"Failed to connect will try again: {ex}")
        sleep(5)
        run_producer()


run_producer()
