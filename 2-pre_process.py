import stanza
import uuid
import re
import json
import datetime
import requests
import os
import uuid

from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

stanza.download('fa')
nlp = stanza.Pipeline('fa')


class DefaultEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, type(uuid.uuid4())):
            return obj.hex
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

with open('./files/stop_wrods.txt', 'r') as f:
    stop_wrods = list(set([x.rstrip() for x in f]))
    
static_keywords = ['بورس', 'اقتصاد', 'تحریم', 'دولت',
                 'حسن روحانی', 'دلار', 'طلا', 'کرونا', 'انتخابات', 'کوید19',
                 'کویئد 19', 'کویید 19', 'دانشگاه', 'تورم']


def pre_process(text):

    id = uuid.uuid4()
    date = datetime.datetime.now()
    doc = nlp(text)
    hashtags = re.findall(r"#(\w+)", text)
    urls = re.findall("(?P<url>https?://[^\s]+)", text)
    keywords = list(set([term.text for term in doc.iter_words() if term.text in static_keywords]))

    return {
        "id":id,
        "source":None,
        "text":text,
        "doc":doc.to_dict(),
        "ctext":None,
        "date_posted":None,
        "date":date,
        "hashtags":hashtags,
        "urls":urls,
        "keywords":keywords
    }

if __name__ == '__main__':

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #consumer = KafkaConsumer('pre-process',value_deserializer=lambda x: loads(x.decode('utf-8')))

    # data = []
    # texts = ['سلام چطوری؟', 'خوبم مرسی', 'لطفا به این لینک مراجعه کنید https://stanfordnlp.github.io/stanza/', 'امروزه #اقتصاد بسیار در گیر بوده است.', 'بیماری کویید 19 خطرناک است']    # recive data from kafka
    consumer = KafkaConsumer('pre-process')
    while consumer:
        consumer = KafkaConsumer('pre-process')
        for msg in consumer:
            text = json.loads(msg.value)
            jtext = json.dumps(pre_process(text), cls=DefaultEncoder)
            # data = {'text' : jtext}
            producer.send('persistence', value=jtext)
    

