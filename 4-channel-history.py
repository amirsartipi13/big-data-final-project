import json
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps


def save_to_casandra(data):
    return data

def show_to_flask(data):
    return None

def pass_to_spark(data):
    return None

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    consumer = KafkaConsumer('channel-history')
    while True:
        consumer = KafkaConsumer('channel-history')
        for msg in consumer:
            data = json.loads(msg.value)
            print(data['text'])
            producer.send('statistics', value=data)
            print("4 -> channel-history send to statistics")
