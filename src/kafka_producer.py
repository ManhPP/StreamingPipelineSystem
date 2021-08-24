import time

from confluent_kafka import Producer
import json
from faker import Faker


fake = Faker()


def get_registered_user():
    return {
        "name": fake.name(),
        "address": fake.address(),
        "created_at": fake.year()
    }


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = Producer(**{"bootstrap.servers": "localhost:9092"})


if __name__ == "__main__":
    print(producer)
    while 1 == 1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.produce("topic1", value=json_serializer(registered_user))
        producer.flush()
        time.sleep(10)

