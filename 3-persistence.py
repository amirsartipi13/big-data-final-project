from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import json


def create_or_get_es(index):
    es = Elasticsearch([{'host': 'localhost', 
                        'requestTimeout': 60000,
                        'port': 9200}],
                        timeout=30)

    try:
        es.indices.get(index=index)
    except:
        es.indices.create(index=index)
    return es

if __name__ == '__main__':

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #consumer = KafkaConsumer('persistance',value_deserializer=lambda x: loads(x.decode('utf-8')))


    es = create_or_get_es('data_center')
    consumer = KafkaConsumer('persistance')
    
    while consumer:
        consumer = KafkaConsumer('persistance')
        for msg in consumer:
            # text = json.loads(msg.value)
            # data = {'text' : jtext}
            helpers.bulk(es, msg, index=index, doc_type="_doc")
            producer.send('channel-history', value=msg)
