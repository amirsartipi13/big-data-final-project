from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import redis, json, datetime

r = redis.Redis(host='localhost', port=6379)

def store_tweet_by_keyword(tweet):
    keywords = tweet["keywords"]
    tweet_id = tweet["id"]
    for keyword in keywords:
        r.set(f"keyword:{keyword}:{tweet_id}", dumps(tweet), ex=60 * 60 * 6)

def store_tweet_by_time(tweet):
    strptime = datetime.datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
    time_key = datetime.datetime.strftime(strptime, '%Y:%m:%d:%H')
    print(time_key)
    r.lpush(f"tweets:{time_key}", dumps(tweet))

def store_last_hour_hashtags(tweet):
    hashtags = tweet["hashtags"]
    for hashtag in hashtags:
        r.set(f"last_hour_hashtags:{hashtag}", hashtag, nx=True)
        r.expire(f"last_hour_hashtags:{hashtag}", 60 * 60)

def store_last_hashtags(tweet):
    hashtags = tweet["hashtags"]
    for hashtag in hashtags:
        r.lpush("last_hashtags", hashtag)
        r.ltrim("last_hashtags", 0, 999)

def store_last_tweets(tweet):
    r.lpush("last_tweets", dumps(tweet))
    r.ltrim("last_tweets", 0, 99)

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer = KafkaConsumer('statistics')

    while True:
        consumer = KafkaConsumer('statistics')
        for msg in consumer:
            tweet = json.loads(msg.value)
            store_tweet_by_keyword(tweet)
            store_tweet_by_time(tweet)
            store_last_hour_hashtags(tweet)
            store_last_hashtags(tweet)
            store_last_tweets(tweet)
            print("stored tweet with id " + str(tweet["id"]))
            # producer.send('exit', value=data)
