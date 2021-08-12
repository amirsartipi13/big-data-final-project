import json
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from time import sleep

with open('./files/stop_wrods.txt', 'r', encoding='utf-8') as f:
    stop_wrods = list(set([x.rstrip() for x in f]))

body={
  "settings": {
    "analysis": {
      "char_filter": {
        "zero_width_spaces": {
            "type":       "mapping",
            "mappings": [ "\\u200C=>\\u0020"] 
        }
      },
      "filter": {
        "persian_stop": {
          "type":       "stop",
          "stopwords":  stop_wrods 
        }
      },
      "analyzer": {
        "rebuilt_persian": {
          "tokenizer":     "standard",
          "char_filter": [ "zero_width_spaces" ],
          "filter": [
            "lowercase",
            "decimal_digit",
            "arabic_normalization",
            "persian_normalization",
            "persian_stop"
          ]
        }
      }
    }
  }
}

def create_or_get_es(index):
    es = Elasticsearch([{'host': 'localhost', 
                        'requestTimeout': 60000,
                        'port': 9200}],
                        timeout=30)

    try:
      es.indices.create(index=index, body=body)
    except:
      es.indices.get(index=index)
  
    return es

if __name__ == '__main__':

    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    es = create_or_get_es('data_center')

    while True:
      consumer = KafkaConsumer('persistence',
                              auto_offset_reset= 'earliest',
                              auto_commit_interval_ms = 1000)
      sleep(3)
      for msg in consumer:
          tweet = json.loads(msg.value)
          es.index('data_center', tweet)
          producer.send('channel-history', value=json.loads(json.loads(msg.value)))
          print("3 -> persistance send to channel-history")