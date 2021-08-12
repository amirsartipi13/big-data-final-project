
from time import sleep
from json import dumps
from kafka import KafkaProducer
from TwitterSearch import *
import random

API_Key = 'LJq2tNmIiojR7gO08cIKxb9tm'
API_Secret_Key = 'B7v9sswv42wfeXy8lnn88WpmRxxo8E7dNQGFcB8Oc4iQ1jW3qC'

Bearer_Token = 'AAAAAAAAAAAAAAAAAAAAAB1USAEAAAAA6jJusoxQiAxt7hlTQNnYwo%2BUnFI%3DRN9w6xyGw9SPE6nZUwpL5lPpfOacHcmi8vvjPlactQ47v4UpLn'
Access_Token = '1419011209307639810-1DYnhpI4kNObmJGukki70DSFgYedoE'
Access_Token_Secret = 'JaAWSFihySWGDuXlgKAAAMwAx1OjXmdLVIAHUrkV5eL74'

f = open('./files/trends.txt', 'r', encoding="utf8")
keywords = f.read()
keywords = keywords.splitlines()
f.close() 

def twitter_crawller(count):
    data = []
    try:
        tso = TwitterSearchOrder()
        index = random.randint(0, len(keywords)-1)	
        # tso.set_keywords(['ایران' , 'المپیک', 'خاورمیانه' , 'کرونا'])
        tso.set_keywords([keywords[index]])
        # tso.set_keywords(keywords)
        # tso.add_keyword(keywords)
        tso.set_language('fa')
        tso.set_include_entities(True)
        tso.set_count(count)
        ts = TwitterSearch(
            consumer_key = API_Key,
            consumer_secret = API_Secret_Key,
            access_token = Access_Token,
            access_token_secret = Access_Token_Secret
        )
        index = 0
        for tweet in ts.search_tweets_iterable(tso):
            index += 1
            data.append(tweet)
            if index==count:
                break

    except TwitterSearchException as e:
        print(e)
    return data

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    for i in range(2):
        data = twitter_crawller(count=10)
        for text in data:
            sleep(3)
            print(text['text'])
            producer.send('pre-process', value=text)
            print("2 -> crawller send to pre-process")