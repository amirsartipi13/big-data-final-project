from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import json


def save_to_casandra(data):

    return data

def show_to_flask(data):
    return None


def pass_to_spark(data):
    return None

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #consumer = KafkaConsumer('channel-history',value_deserializer=lambda x: loads(x.decode('utf-8')))

    consumer = KafkaConsumer('channel-history')
    while consumer:
        consumer = KafkaConsumer('persistance')
        for msg in consumer:
            data = json.loads(msg.value)
            save_to_casandra(data)
            show_to_flask(data)
            pass_to_spark()
            producer.send('statistics', value=data)
