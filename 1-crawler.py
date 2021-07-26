
from time import sleep
from json import dumps
from kafka import KafkaProducer


def crawller():
    text = ['این تکس مثلا  کرال شده است.']
    retrun text


if __name__ == '__main__':
    
    last_time = ''
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    while(True):
        texts = crawller()
        for text in texts:
            data = {'text' : text}
            producer.send('pre-process', value=data)
