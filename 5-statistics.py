from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import json


def save_to_redis(data):

    return data

def get_redis_connection():
    return 'Connection'

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #consumer = KafkaConsumer('statistics',value_deserializer=lambda x: loads(x.decode('utf-8')))
    consumer = KafkaConsumer('statistics')
    # connect to the redis
    redis = get_redis_connection()
    while consumer:
        consumer = KafkaConsumer('statistics')
        for msg in consumer:
            data = json.loads(msg.value)
            save_to_redis(data)
            producer.send('exit', value=data)
