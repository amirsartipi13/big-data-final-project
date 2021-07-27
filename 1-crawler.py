
from time import sleep
from json import dumps
from kafka import KafkaProducer
from TwitterSearch import *

API_Key = 'Eo3ej5ML2UbkkJUEcF78lEyOA'
API_Secret_Key = 'Q1ED20LEAhrgWkldBPP7QCy1LO0As24y5Kp2KlgedJPxTqm0II'


Bearer_Token = 'AAAAAAAAAAAAAAAAAAAAAB1USAEAAAAAEfCgv2RY0waIj7mCpnNDmIT70cM%3D99Yx4oDNnX8Ky2oUYeuqvANZRf5YwlQIO5zH7zWD5dRC5Zsdvs'
Access_Token = '1419011209307639810-nI6S77Lvja1XpRftPNI2YcBjleiJvV'
Access_Token_Secret = 'tyTVLcojbETSeq0NFfoA8UHPBQdf50WoDYpnEgXU5m2S2'



def twitter_crawller(count=10):
    data = []
    try:
        tso = TwitterSearchOrder()
        tso.set_keywords(['ایران'])
        tso.set_language('fa')
        tso.set_include_entities(True)
        tso.set_count(count)

        ts = TwitterSearch(
            consumer_key = API_Key,
            consumer_secret = API_Secret_Key,
            access_token = Access_Token,
            access_token_secret = Access_Token_Secret
        )

        for tweet in ts.search_tweets_iterable(tso):
            # print( '@%s tweeted: %s' % ( tweet['user']['screen_name'], tweet['text'] ) )
            data.append(tweet)
    except TwitterSearchException as e:
        print(e)

    return data


if __name__ == '__main__':
    
    last_time = ''
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    for i in range(2):
        texts = twitter_crawller(count=10):
        sleep(5)
        for text in data:
            sleep(2)
            producer.send('pre-process', value=text)
